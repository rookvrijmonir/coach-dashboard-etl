import os
import sys
from dotenv import load_dotenv
from hubspot import HubSpot

# Configuratie laden
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

def get_client():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Check je .env bestand!")
        sys.exit(1)
    return HubSpot(access_token=token)

def search_property(client, object_type, search_term):
    print(f"\n--- Zoeken naar '{search_term}' in {object_type.upper()} ---")
    try:
        # Haal alle properties op van dit object type
        response = client.crm.properties.core_api.get_all(object_type=object_type)
        
        found = False
        for prop in response.results:
            # We zoeken in de label (leesbare naam) en de name (interne naam)
            if search_term.lower() in prop.label.lower() or search_term.lower() in prop.name.lower():
                print(f"✅ GEVONDEN!")
                print(f"   Label (wat jij ziet):  {prop.label}")
                print(f"   Interne naam (code):   {prop.name}")
                print(f"   Type:                  {prop.type}")
                print("-" * 30)
                found = True
        
        if not found:
            print(f"❌ Niets gevonden dat lijkt op '{search_term}'.")
            
    except Exception as e:
        print(f"Fout bij ophalen properties: {e}")

if __name__ == "__main__":
    client = get_client()
    
    # 1. BESTAANDE ZOEKTOCHTEN (Die we al hadden)
    # search_property(client, "deals", "coach")
    # search_property(client, "deals", "begeleiding")
    
    # 2. NIEUW: ZOEK NAAR TIJD & STAGNATIE VELDEN
    # Zoek naar velden die "date entered" bevatten (wanneer kwam de deal in deze fase?)
    search_property(client, "deals", "date_entered")
    
    # Zoek naar "last contact" of "laatste contact" (wanneer heeft de coach iets gedaan?)
    search_property(client, "deals", "last_sales_activity")
    search_property(client, "deals", "notes_last_updated")
    
    # Zoek naar "time in" (hoe lang zit hij al in deze fase?)
    search_property(client, "deals", "time_in")