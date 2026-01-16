import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- PAGINA CONFIGURATIE ---
st.set_page_config(
    page_title="Coach Load Balancer",
    page_icon="📊",
    layout="wide"
)

# --- FUNCTIES ---
def load_data():
    """Laadt de data uit de lokale CSV export."""
    # We gaan er vanuit dat main.py dit bestand heeft aangemaakt in de 'data' map
    file_path = os.path.join('data', 'hubspot_export_raw.csv')
    
    if not os.path.exists(file_path):
        st.error(f"❌ Geen data gevonden op pad: {file_path}")
        st.info("💡 Tip: Draai eerst 'python src/main.py' om de data uit HubSpot te halen.")
        return None
        
    try:
        # Lees CSV (let op: puntkomma als scheidingsteken, want dat doet main.py ook)
        df = pd.read_csv(file_path, sep=';')
        
        # Datums omzetten naar datetime objecten voor rekenwerk
        date_cols = [
            'createdate', 
            'laatste_activiteit', 
            'datum_in_fase', 
            'datum_naar_nabeller', 
            'datum_in_gebeld_geen_gehoor'
        ]
        
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        return df
    except Exception as e:
        st.error(f"Fout bij inlezen data: {e}")
        return None

def calculate_kpis(df):
    """Berekent de belangrijkste metrics voor bovenin het dashboard."""
    totaal_deals = len(df)
    
    # 1. Verdrink Ratio (Leads naar nabeller)
    # We kijken of er een datum is ingevuld bij 'datum_naar_nabeller'
    if 'datum_naar_nabeller' in df.columns:
        verdronken = df[df['datum_naar_nabeller'].notna()]
        aantal_verdronken = len(verdronken)
    else:
        aantal_verdronken = 0
        
    verdrink_ratio = (aantal_verdronken / totaal_deals * 100) if totaal_deals > 0 else 0
    
    # 2. Stagnatie (Voorbeeld: langer dan 14 dagen in huidige fase)
    aantal_stagnatie = 0
    if 'datum_in_fase' in df.columns:
        nu = pd.Timestamp.now(tz='UTC')
        # Zorg voor timezone awareness als de dataframe dat niet heeft
        if df['datum_in_fase'].dt.tz is None:
             df['datum_in_fase'] = df['datum_in_fase'].dt.tz_localize('UTC')
        
        # Tel aantal deals dat langer dan 14 dagen in de fase zit
        dagen_in_fase = (nu - df['datum_in_fase']).dt.days
        aantal_stagnatie = dagen_in_fase[dagen_in_fase > 14].count()

    return totaal_deals, aantal_verdronken, verdrink_ratio, aantal_stagnatie

# --- DASHBOARD START ---
st.title("📊 Coach Load & Performance Dashboard")
st.markdown("Lokaal prototype op basis van HubSpot export.")

df = load_data()

if df is not None:
    # --- FILTERS (Sidebar) ---
    st.sidebar.header("Filters")
    
    # Filter 1: Deal Fase
    if 'dealstage' in df.columns:
        fases = ["Alle"] + sorted(list(df['dealstage'].dropna().unique()))
        gekozen_fase = st.sidebar.selectbox("Kies Fase:", fases)
        
        if gekozen_fase != "Alle":
            df_filtered = df[df['dealstage'] == gekozen_fase]
        else:
            df_filtered = df
    else:
        df_filtered = df

    # --- KPI BLOKKEN ---
    totaal, verdronken, ratio, stagnatie = calculate_kpis(df_filtered)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Aantal Deals", totaal)
    col2.metric("Naar Nabeller (Verdronken)", verdronken, delta_color="inverse")
    col3.metric("Verdrink Ratio", f"{ratio:.1f}%", delta_color="inverse")
    col4.metric("Stagnatie (>14 dgn)", stagnatie, delta_color="inverse")
    
    st.divider()

    # --- TABBLADEN VOOR DIEPGANG ---
    tab1, tab2, tab3 = st.tabs(["📈 Verdrink Analyse", "🚨 Stagnatie", "🗺️ Regio & Postcode"])
    
    # TAB 1: VERDRINK RATIO (Wie verliest leads?)
    with tab1:
        st.subheader("Verdrink Analyse: Wie verliest leads?")
        st.markdown("*Dit toont welke 'Broncoach' de lead had voordat hij naar de nabeller ging.*")
        
        if 'broncoach' in df_filtered.columns:
             # We tellen alleen de records waar 'broncoach' is ingevuld (want dat zijn de 'verliezers')
             verlies_per_coach = df_filtered[df_filtered['broncoach'].notna()]['broncoach'].value_counts().reset_index()
             
             if not verlies_per_coach.empty:
                 verlies_per_coach.columns = ['Coach', 'Aantal Verloren']
                 fig = px.bar(verlies_per_coach, x='Coach', y='Aantal Verloren', 
                              title="Leads doorgezet naar Nabeller per Coach",
                              color='Aantal Verloren', color_continuous_scale='Reds')
                 st.plotly_chart(fig, use_container_width=True)
             else:
                 st.success("Geen 'broncoach' data gevonden in de huidige selectie. Goed teken! (Of de kolom is leeg)")
        else:
             st.warning("Kolom 'broncoach' ontbreekt in dataset.")

    # TAB 2: STAGNATIE (Wie is traag?)
    with tab2:
        st.subheader("Stagnatie Analyse")
        
        if 'datum_in_fase' in df_filtered.columns:
            nu = pd.Timestamp.now(tz='UTC')
            # Timezone fix (veiligheidshalve nogmaals)
            if df_filtered['datum_in_fase'].dt.tz is None:
                 df_filtered['datum_in_fase'] = df_filtered['datum_in_fase'].dt.tz_localize('UTC')
            
            # Bereken dagen
            df_filtered['dagen_in_fase'] = (nu - df_filtered['datum_in_fase']).dt.days
            
            # Histogram
            fig_hist = px.histogram(df_filtered, x="dagen_in_fase", nbins=20, 
                                    title="Verdeling: Hoe lang staan deals stil?",
                                    labels={'dagen_in_fase': 'Dagen in huidige fase'})
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Top 10 Tabel
            st.write("⚠️ Top 10 Langstlopende Deals in huidige selectie:")
            
            show_cols = ['deal_name', 'dealstage', 'dagen_in_fase']
            # Voeg extra kolommen toe als ze bestaan voor context
            if 'laatste_activiteit' in df_filtered.columns: show_cols.append('laatste_activiteit')
            if 'broncoach' in df_filtered.columns: show_cols.append('broncoach')

            st.dataframe(
                df_filtered[show_cols]
                .sort_values('dagen_in_fase', ascending=False)
                .head(10)
            )
        else:
            st.warning("Geen datum velden voor stagnatie gevonden.")

    # TAB 3: REGIO (Waar zitten ze?)
    with tab3:
        st.subheader("Geografische Spreiding")
        
        if 'postcode' in df_filtered.columns:
            # Maak een kopie om warnings te voorkomen
            df_geo = df_filtered.copy()
            
            # Schoonmaken: Haal lege postcodes weg en maak string
            df_geo = df_geo.dropna(subset=['postcode'])
            df_geo['postcode'] = df_geo['postcode'].astype(str)
            
            # Eerste 2 cijfers pakken (PC2 regio)
            df_geo['regio'] = df_geo['postcode'].str[:2]
            
            # Tellen
            regio_counts = df_geo['regio'].value_counts().reset_index()
            regio_counts.columns = ['Regio (PC2)', 'Aantal']
            
            # Sorteren op aantal
            regio_counts = regio_counts.sort_values('Aantal', ascending=False)
            
            fig_map = px.bar(regio_counts, x='Regio (PC2)', y='Aantal', 
                             title="Deals per Regio (Eerste 2 cijfers postcode)")
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.warning("Geen postcodes gevonden.")

    # RUWE DATA (Altijd handig voor debuggen)
    with st.expander("🔍 Toon Ruwe Data Tabel"):
        st.dataframe(df_filtered)