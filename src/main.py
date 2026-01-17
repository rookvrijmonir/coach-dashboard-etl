import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException

# --- CONFIGURATIE ---
# We laden de .env vanuit de root map
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# DE VOLLEDIGE LIJST MET PROPERTIES
DEAL_PROPS = [
    "dealname", "dealstage", "pipeline", "createdate", "hubspot_owner_id",
    "verzekeraar", "hoeveelheid_begeleiding",
    "vgz_voldoende_begeleiding",
    "dsw_1e_sessie_is_geweest",
    "datum_ig",
    "hs_v2_date_entered_114855767", 
    "hs_v2_date_entered_15413223", 
    "hs_v2_date_entered_15413226"  
]

def get_client():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Check je .env bestand!")
        sys.exit(1)
    return HubSpot(access_token=token)

def get_owner_map(client):
    owner_map = {}
    try:
        owners = client.crm.owners.get_all()
        for owner in owners:
            owner_map[str(owner.id)] = f"{owner.first_name} {owner.last_name}".strip()
    except Exception as e:
        print(f"⚠️ Kon owners niet ophalen: {e}")
    return owner_map

def run_pipeline():
    client = get_client()
    owner_map = get_owner_map(client)
    
    print("\n" + "="*50)
    print("🚀 HubSpot Data Extractie (Met Rate-Limit Recovery)")
    print("="*50)

    current_start = datetime(2025, 3, 1)
    end_goal = datetime.now()
    all_rows = []
    
    while current_start < end_goal:
        current_end = current_start + timedelta(days=14)
        print(f"📅 Periode: {current_start.date()} tot {current_end.date()}...")
        
        after = 0
        while True:
            search_request = PublicObjectSearchRequest(
                filter_groups=[{
                    "filters": [
                        {"propertyName": "createdate", "operator": "GTE", "value": int(current_start.timestamp() * 1000)},
                        {"propertyName": "createdate", "operator": "LT", "value": int(current_end.timestamp() * 1000)}
                    ]
                }],
                properties=DEAL_PROPS, limit=100, after=after
            )
            
            # --- RETRY LOGICA VOOR RATE LIMITS (429) ---
            success = False
            retries = 0
            max_retries = 5
            
            while not success and retries < max_retries:
                try:
                    resp = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
                    
                    for d in resp.results:
                        p = d.properties
                        owner_id = str(p.get("hubspot_owner_id", ""))
                        all_rows.append({
                            "deal_id": d.id,
                            "dealname": p.get("dealname"),
                            "createdate": p.get("createdate"),
                            "dealstage": p.get("dealstage"),
                            "verzekeraar": p.get("verzekeraar"),
                            "begeleiding": p.get("hoeveelheid_begeleiding"),
                            "vgz_voldoende": p.get("vgz_voldoende_begeleiding"),
                            "dsw_sessie": p.get("dsw_1e_sessie_is_geweest"),
                            "datum_ig": p.get("datum_ig"),
                            "coach_naam": owner_map.get(owner_id, "Onbekend"),
                            "ts_warme_aanvraag": p.get("hs_v2_date_entered_114855767"),
                            "ts_in_begeleiding": p.get("hs_v2_date_entered_15413223"),
                            "datum_afgesloten": p.get("hs_v2_date_entered_15413226")
                        })
                    
                    if not resp.paging or not resp.paging.next:
                        after = None
                    else:
                        after = resp.paging.next.after
                    
                    success = True
                    
                except ApiException as e:
                    if e.status == 429:
                        retries += 1
                        wait_time = 2 ** retries # Wacht 2, 4, 8, 16 seconden
                        time.sleep(wait_time)
                    else:
                        print(f"❌ HubSpot API Fout: {e}")
                        break
                except Exception as e:
                    print(f"❌ Onbekende fout: {e}")
                    break

            if after is None or not success:
                break
        
        current_start = current_end

    if all_rows:
        df = pd.DataFrame(all_rows)
        os.makedirs('data', exist_ok=True)
        df.to_csv('data/hubspot_export_raw.csv', index=False, sep=';')
        print(f"✅ Export succesvol: {len(df)} deals opgeslagen in data/hubspot_export_raw.csv")

if __name__ == "__main__":
    run_pipeline()