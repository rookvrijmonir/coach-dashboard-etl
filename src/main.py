import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
from hubspot.crm.associations import BatchInputPublicObjectId
from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId

# --- CONFIGURATIE ---
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

DEAL_PROPS = [
    "dealname", "dealstage", "pipeline", "createdate", "hubspot_owner_id",
    "verzekeraar", "broncoach_tekst", "hoeveelheid_begeleiding",
    "datum_afgesloten", "hs_v2_date_entered_15413226", 
    "ts_warme_aanvraag", "ts_info_aangevraagd", "ts_in_begeleiding",
    # We halen ook de interne tijdstempels op die we in de CSV zagen
    "hs_v2_date_entered_114855767", # Warme aanvraag
    "hs_v2_date_entered_15413223"   # In begeleiding
]

def get_client():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token: print("FOUT: Geen token!"); sys.exit(1)
    return HubSpot(access_token=token)

def get_owner_map(client):
    """Haalt alle HubSpot Owners op om ID's naar Namen te vertalen."""
    print("👤 Owners ophalen uit HubSpot...")
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
    user_input = input("🔢 Hoeveel deals ophalen? (Leeg = ALLES): ").strip()
    max_limit = int(user_input) if user_input.isdigit() else None
    print("="*50 + "\n")

    # Start vanaf maart 2025 (zoals in je eerdere vraag)
    current_start = datetime(2025, 3, 1)
    end_goal = datetime.now()
    
    all_rows = []
    total_count = 0
    
    while current_start < end_goal:
        if max_limit and total_count >= max_limit: break
        current_end = current_start + timedelta(days=14)
        print(f"📅 Periode: {current_start.date()} tot {current_end.date()}...")
        
        after = 0
        while True:
            if max_limit and total_count >= max_limit: break
            
            search_request = PublicObjectSearchRequest(
                filter_groups=[{
                    "filters": [
                        {"propertyName": "createdate", "operator": "GTE", "value": int(current_start.timestamp() * 1000)},
                        {"propertyName": "createdate", "operator": "LT", "value": int(current_end.timestamp() * 1000)}
                    ]
                }],
                properties=DEAL_PROPS, limit=100, after=after
            )
            
            try:
                resp = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
                if not resp.results: break
                
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
                        # Gebruik de Owner Name als Coach, fallback op broncoach_tekst
                        "coach_naam": owner_map.get(owner_id, p.get("broncoach_tekst", "Onbekend")),
                        "ts_warme_aanvraag": p.get("hs_v2_date_entered_114855767") or p.get("createdate"),
                        "ts_in_begeleiding": p.get("hs_v2_date_entered_15413223"),
                        "datum_afgesloten": p.get("hs_v2_date_entered_15413226")
                    })
                
                total_count += len(resp.results)
                if not resp.paging or not resp.paging.next: break
                after = resp.paging.next.after
            except Exception as e:
                print(f"❌ Fout: {e}"); break
        
        current_start = current_end

    if all_rows:
        df = pd.DataFrame(all_rows)
        os.makedirs('data', exist_ok=True)
        df.to_csv('data/hubspot_export_raw.csv', index=False, sep=';')
        print(f"✅ Export klaar: {len(df)} deals met coach-namen.")

if __name__ == "__main__":
    run_pipeline()