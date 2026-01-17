import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId

# --- CONFIGURATIE ---
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# Dealeigenschappen (met de recordsleutel die jij aangaf)
DEAL_PROPS = [
    "dealname", "dealstage", "hubspot_owner_id", "createdate",
    "verzekeraar", "hoeveelheid_begeleiding", "record_id_contactpersoon",
    "vgz_voldoende_begeleiding", "dsw_1e_sessie_is_geweest", "datum_ig",
    "hs_v2_date_entered_114855767", "hs_v2_date_entered_15413223", "hs_v2_date_entered_15413226"
]

# Contacteigenschappen (van het deelnemer-record)
CONTACT_PROPS = ["aangebracht_door", "zip", "geslacht", "geboortejaar"]

def get_client():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Geen token gevonden in .env"); sys.exit(1)
    return HubSpot(access_token=token)

def run_pipeline():
    client = get_client()
    print("\n" + "="*50)
    print("🚀 UNIFIED ETL ENGINE: Deals & Contacts (Batched)")
    print("="*50)

    # 1. Owners ophalen voor coachnamen
    try:
        owner_map = {str(o.id): f"{o.first_name} {o.last_name}".strip() for o in client.crm.owners.get_all()}
    except Exception:
        owner_map = {}

    current_start = datetime(2025, 3, 1)
    end_goal = datetime.now()
    all_deals = []
    
    # 2. Ophalen Deals (Batchgewijs door de tijd)
    while current_start < end_goal:
        current_end = current_start + timedelta(days=14)
        print(f"📅 Periode: {current_start.date()} tot {current_end.date()}...")
        after = 0
        while True:
            search_request = PublicObjectSearchRequest(
                filter_groups=[{"filters": [
                    {"propertyName": "createdate", "operator": "GTE", "value": int(current_start.timestamp() * 1000)},
                    {"propertyName": "createdate", "operator": "LT", "value": int(current_end.timestamp() * 1000)}
                ]}],
                properties=DEAL_PROPS, limit=100, after=after
            )
            
            try:
                resp = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
                if not resp.results: break
                for d in resp.results:
                    all_deals.append(d.properties)
                after = resp.paging.next.after if resp.paging and resp.paging.next else None
                if not after: break
            except ApiException as e:
                if e.status == 429:
                    print("⏳ Rate limit. Wachten..."); time.sleep(5)
                else: break
        current_start = current_end

    if not all_deals:
        print("Geen deals gevonden."); return

    # 3. Ophalen Contacten via record_id_contactpersoon (Batched)
    contact_ids = list(set([d.get("record_id_contactpersoon") for d in all_deals if d.get("record_id_contactpersoon")]))
    contact_data = {}
    print(f"🧬 Koppelen van {len(contact_ids)} contactpersonen...")
    
    for i in range(0, len(contact_ids), 100):
        batch_ids = contact_ids[i:i+100]
        # BELANGRIJK: Properties moeten IN de BatchReadInputSimplePublicObjectId
        batch_input = BatchReadInputSimplePublicObjectId(
            inputs=[{"id": cid} for cid in batch_ids],
            properties=CONTACT_PROPS
        )
        try:
            c_resp = client.crm.contacts.batch_api.read(batch_read_input_simple_public_object_id=batch_input)
            for c in c_resp.results:
                contact_data[str(c.id)] = c.properties
        except Exception as e:
            print(f"⚠️ Batch fout: {e}")

    # 4. Samenvoegen tot finale CSV
    final_rows = []
    for d in all_deals:
        c_id = str(d.get("record_id_contactpersoon", ""))
        cp = contact_data.get(c_id, {})
        
        final_rows.append({
            "deal_id": d.get("hs_object_id"),
            "dealname": d.get("dealname"),
            "createdate": d.get("createdate"),
            "dealstage": d.get("dealstage"),
            "coach_naam": owner_map.get(str(d.get("hubspot_owner_id", "")), "Onbekend"),
            "verzekeraar": d.get("verzekeraar"),
            "begeleiding": d.get("hoeveelheid_begeleiding"),
            "vgz_voldoende": d.get("vgz_voldoende_begeleiding"),
            "dsw_sessie": d.get("dsw_1e_sessie_is_geweest"),
            "datum_ig": d.get("datum_ig"),
            # Velden uit Contact
            "postcode": cp.get("zip"),
            "aangebracht_door": cp.get("aangebracht_door"),
            "geslacht": cp.get("geslacht"),
            "geboortejaar": cp.get("geboortejaar"),
            # Timestamps voor benchmark
            "ts_warme_aanvraag": d.get("hs_v2_date_entered_114855767"),
            "ts_in_begeleiding": d.get("hs_v2_date_entered_15413223"),
            "datum_afgesloten": d.get("hs_v2_date_entered_15413226")
        })

    df = pd.DataFrame(final_rows)
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/hubspot_export_raw.csv', index=False, sep=';')
    print(f"✅ Succes! {len(df)} dossiers opgeslagen in data/hubspot_export_raw.csv")

if __name__ == "__main__":
    run_pipeline()