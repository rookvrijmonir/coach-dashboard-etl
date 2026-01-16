import os
import sys
import re
import pandas as pd
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
from hubspot.crm.associations import BatchInputPublicObjectId, ApiException as AssociationException
from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId, ApiException as ContactException

# --- CONFIGURATIE ---
# We zoeken het .env bestand in de hoofdmap
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# 1. DEAL EIGENSCHAPPEN (Wat we ophalen)
DEAL_PROPS = [
    # Basis
    "dealname", "amount", "dealstage", "pipeline", "hubspot_owner_id", 
    "createdate", "closedate",
    
    # Tijd & Activiteit (Standaard HubSpot velden die altijd bestaan)
    "hs_last_sales_activity_date",  # Wanneer was laatste actie coach?
    "hs_date_entered_dealstage",    # Wanneer in de HUIDIGE fase gekomen?
    
    # Custom (Uit jouw bestanden/input)
    "broncoach_tekst", 
    "hoeveelheid_begeleiding", 
    "mag_gedeclareerd_worden",
    "datum_naar_nabeller",

    # Specifieke stagnatie (gevonden met jouw zoektocht)
    "hs_v2_date_entered_15415583" # Datum in "Gebeld, geen gehoor"
]

# 2. CONTACT Eigenschappen
CONTACT_PROPS = [
    "firstname", "lastname", "email", "zip", "city", 
    "verzekeraar" 
]

def clean_bsn(text):
    """Privacy: Verwijder BSN patronen (8-9 cijfers) uit tekst."""
    if not isinstance(text, str): return text
    return re.sub(r'\b\d{8,9}\b', '[BSN_VERWIJDERD]', text)

def get_client():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Check je .env bestand! Token ontbreekt.")
        sys.exit(1)
    return HubSpot(access_token=token)

def get_associations_batch(client, deal_ids):
    """Haalt Contact ID's op die bij een lijst Deals horen."""
    deal_to_contact = {}
    if not deal_ids: return deal_to_contact

    try:
        batch_input = BatchInputPublicObjectId(inputs=deal_ids)
        response = client.crm.associations.batch_api.read(
            from_object_type="deal",
            to_object_type="contact",
            batch_input_public_object_id=batch_input
        )
        for result in response.results:
            if result.to and len(result.to) > 0:
                deal_to_contact[result._from.id] = result.to[0].id
    except AssociationException as e:
        print(f"⚠️ Kon associaties niet ophalen: {e}")

    return deal_to_contact

def get_contacts_details_batch(client, contact_ids):
    """Haalt Contact Details (Postcode, Verzekeraar) op."""
    contact_data = {}
    unique_ids = list(set(contact_ids))
    if not unique_ids: return contact_data

    chunk_size = 100
    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]
        try:
            batch_input = BatchReadInputSimplePublicObjectId(
                inputs=[{"id": uid} for uid in chunk],
                properties=CONTACT_PROPS
            )
            response = client.crm.contacts.batch_api.read(
                batch_read_input_simple_public_object_id=batch_input
            )
            for contact in response.results:
                contact_data[contact.id] = contact.properties
        except ContactException as e:
            print(f"⚠️ Fout bij ophalen contact details: {e}")
    return contact_data

def run_pipeline():
    client = get_client()
    start_timestamp = os.getenv("START_DATE_TIMESTAMP")
    
    print(f"--- START DATA EXTRACTIE (Opslaan naar CSV) ---")
    
    all_rows = []
    after = 0
    batch_size = 50 
    total_processed = 0

    while True:
        # A. Deals Ophalen
        search_request = PublicObjectSearchRequest(
            filter_groups=[{
                "filters": [{
                    "propertyName": "createdate",
                    "operator": "GTE",
                    "value": start_timestamp
                }]
            }],
            properties=DEAL_PROPS,
            limit=batch_size,
            after=after,
            sorts=["createdate"]
        )
        
        try:
            deal_response = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
            deals = deal_response.results
            
            if not deals:
                break
                
            print(f"📦 Batch verwerken: {len(deals)} deals...")
            
            # B. Contacten Koppelen
            deal_ids = [d.id for d in deals]
            deal_to_contact_map = get_associations_batch(client, deal_ids)
            
            contact_ids_to_fetch = list(deal_to_contact_map.values())
            contact_details_map = get_contacts_details_batch(client, contact_ids_to_fetch)

            # C. Data Samenvoegen
            for deal in deals:
                d_props = deal.properties
                contact_id = deal_to_contact_map.get(deal.id)
                c_props = contact_details_map.get(contact_id, {}) if contact_id else {}
                
                verzekeraar_clean = clean_bsn(c_props.get("verzekeraar"))

                row = {
                    "deal_id": deal.id,
                    "deal_name": d_props.get("dealname"),
                    "createdate": d_props.get("createdate"),
                    "dealstage": d_props.get("dealstage"),
                    
                    # Tijd & Activiteit (Voor stagnatie dashboard)
                    "laatste_activiteit": d_props.get("hs_last_sales_activity_date"),
                    "datum_in_fase": d_props.get("hs_date_entered_dealstage"),
                    "datum_in_gebeld_geen_gehoor": d_props.get("hs_v2_date_entered_15415583"),
                    
                    # Business Logic (Jouw velden)
                    "broncoach": d_props.get("broncoach_tekst"), 
                    "begeleiding": d_props.get("hoeveelheid_begeleiding"),
                    "declarabel": d_props.get("mag_gedeclareerd_worden"),
                    "datum_naar_nabeller": d_props.get("datum_naar_nabeller"),
                    
                    # Locatie & Verzekering
                    "postcode": c_props.get("zip"),
                    "stad": c_props.get("city"),
                    "verzekeraar": verzekeraar_clean
                }
                all_rows.append(row)
            
            total_processed += len(deals)

            if not deal_response.paging or not deal_response.paging.next:
                break
            after = deal_response.paging.next.after
            
            # VEILIGHEID: Zet deze limiet HOGER of UIT als je alles wilt hebben
            if total_processed >= 500: 
                print("🛑 TEST STOP: Limiet van 500 bereikt. Verhoog dit in de code voor meer.")
                break

        except ApiException as e:
            print(f"❌ CRASH: {e}")
            break

    # D. Opslaan naar Bestand
    if all_rows:
        df = pd.DataFrame(all_rows)
        
        # Zorg dat de map 'data' bestaat
        os.makedirs('data', exist_ok=True)
        
        output_file = 'data/hubspot_export_raw.csv'
        df.to_csv(output_file, index=False, sep=';')
        
        print(f"\n✅ SUCCES! Data opgeslagen in: {output_file}")
        print(f"   Totaal aantal rijen: {len(df)}")
        print(f"   Pad: {os.path.abspath(output_file)}")
        print("\nJe kunt dit bestand nu openen in Excel om te controleren.")
    else:
        print("Geen data gevonden.")

if __name__ == "__main__":
    run_pipeline()