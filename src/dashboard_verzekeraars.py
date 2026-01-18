import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime

# ================= CONFIGURATIE =================
st.set_page_config(
    page_title="InsurTech Analytics - Detail",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling
st.markdown("""
<style>
    .metric-card { background-color: #f8f9fa; border-left: 5px solid #2c3e50; padding: 15px; }
    .stTabs [aria-selected="true"] { background-color: #2c3e50; color: white; }
</style>
""", unsafe_allow_html=True)

# Tarieven
TARIEVEN = {"Volledig": 205.00, "Deel/Dropout": 102.50, "Nog niet declarabel": 0.00}

# ================= DATA ENGINE =================
@st.cache_data
def load_data():
    file_path = os.path.join("data", "hubspot_export_raw.csv")
    try:
        df = pd.read_csv(file_path, sep=";")
    except FileNotFoundError:
        st.error(f"❌ Bestand '{file_path}' niet gevonden.")
        return pd.DataFrame()

    # 1. Datums
    date_cols = ['createdate', 'mag_gedeclareerd_worden_datum', 'closedate', 'datum_ig']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    # 2. VERZEKERAAR SCHOONMAKEN (Cruciaal: Geen groepen meer!)
    if 'verzekeraar' in df.columns:
        # Alles naar tekst, spaties weg, en netjes Hoofdletters (zodat 'cz' == 'CZ')
        df['Verzekeraar'] = df['verzekeraar'].astype(str).str.strip()
        
        # Correcties voor veelvoorkomende variaties (optioneel, kan je uitbreiden)
        df['Verzekeraar'] = df['Verzekeraar'].replace({
            'nan': 'Onbekend',
            'None': 'Onbekend',
            'univé': 'Unive', # Consistentie
            'zk': 'Zilveren Kruis',
            'achmea': 'Zilveren Kruis' 
        })
        
        # Zorg dat lege velden 'Onbekend' heten
        df.loc[df['Verzekeraar'] == '', 'Verzekeraar'] = 'Onbekend'
    else:
        df['Verzekeraar'] = "Onbekend"

    # 3. Status Bepaling
    def determine_status(row):
        if pd.notna(row.get('mag_gedeclareerd_worden_datum')):
            begeleiding = str(row.get('begeleiding', '')).lower()
            if 'volledig' in begeleiding or '100' in begeleiding:
                return "Volledig"
            return "Deel/Dropout"
        
        status = str(row.get('status_bucket', '')).lower()
        if 'verloren' in status:
            return "Verloren (Niet declarabel)"
        return "Lopende Pipeline"

    df['Dossier_Status'] = df.apply(determine_status, axis=1)
    df['Is_Declarabel'] = df['Dossier_Status'].isin(["Volledig", "Deel/Dropout"])
    
    # 4. Waarde & Doorlooptijd
    df['Waarde_Potentieel'] = df['Dossier_Status'].map(TARIEVEN).fillna(0)
    df['Dagen_Tot_Declarabel'] = (df['mag_gedeclareerd_worden_datum'] - df['createdate']).dt.days

    # 5. Leeftijd (indien beschikbaar)
    if 'geboortejaar' in df.columns:
        df['Leeftijd'] = datetime.now().year - pd.to_numeric(df['geboortejaar'], errors='coerce')

    return df

df_raw = load_data()
if df_raw.empty: st.stop()

# ================= FILTERS =================
with st.sidebar:
    st.title("Filters")
    
    # Datum
    if not df_raw['createdate'].isna().all():
        min_d, max_d = df_raw['createdate'].min().date(), df_raw['createdate'].max().date()
        start_date, end_date = st.date_input("📅 Periode", [min_d, max_d], min_value=min_d, max_value=max_d)
    
    # Verzekeraar Selectie (Nu op echte naam!)
    # Sorteer op volume zodat de kleintjes onderaan staan
    top_insurers = df_raw['Verzekeraar'].value_counts().index.tolist()
    sel_insurers = st.multiselect("🏥 Verzekeraar", top_insurers, default=top_insurers[:10]) # Default top 10
    
    # Filter toepassen
    mask = (
        (df_raw['createdate'].dt.date >= start_date) & 
        (df_raw['createdate'].dt.date <= end_date) &
        (df_raw['Verzekeraar'].isin(sel_insurers))
    )
    df = df_raw[mask].copy()

# ================= DASHBOARD =================
st.title("🎯 Verzekeraars Detail Dashboard")
st.markdown(f"**Focus:** Prestaties per individueel label (Geen groepen)")

# KPI's (Alleen Declarabele Dossiers)
df_decl = df[df['Is_Declarabel']].copy()

k1, k2, k3, k4 = st.columns(4)
totaal_waarde = df_decl['Waarde_Potentieel'].sum()
aantal_decl = len(df_decl)
avg_days = df_decl['Dagen_Tot_Declarabel'].mean()
dropout_rate = (len(df_decl[df_decl['Dossier_Status'] == 'Deel/Dropout']) / aantal_decl * 100) if aantal_decl > 0 else 0

k1.metric("💰 Omzet (Declarabel)", f"€ {totaal_waarde:,.0f}".replace(",", "."))
k2.metric("⏱️ Gem. Doorlooptijd", f"{avg_days:.1f} dagen")
k3.metric("⚠️ Dropout Rate", f"{dropout_rate:.1f}%", delta_color="inverse")
k4.metric("📂 Aantal Dossiers", aantal_decl)

st.divider()

# --- TABBLADEN ---
tab1, tab2, tab3 = st.tabs(["🔥 Dropout Ranking", "⏱️ Snelheid & Proces", "📊 Volume & Waarde"])

# === TAB 1: DROPOUT RANKING ===
with tab1:
    st.subheader("Welke verzekeraar heeft de meeste uitvallers?")
    st.markdown("*Percentage dossiers dat eindigt in 'Deel/Dropout' in plaats van 'Volledig'*")
    
    # Bereken dropout % per verzekeraar
    # Filter op minstens 5 dossiers om scheve statistiek (1 uit 1 = 100%) te voorkomen
    v_counts = df_decl['Verzekeraar'].value_counts()
    valid_insurers = v_counts[v_counts >= 5].index
    df_zoom = df_decl[df_decl['Verzekeraar'].isin(valid_insurers)]
    
    drop_stats = df_zoom.groupby('Verzekeraar')['Dossier_Status'].value_counts(normalize=True).unstack().fillna(0)
    
    if 'Deel/Dropout' in drop_stats.columns:
        drop_stats = (drop_stats['Deel/Dropout'] * 100).reset_index(name='Dropout_Pct')
        drop_stats = drop_stats.sort_values('Dropout_Pct', ascending=False).head(20)
        
        fig_drop = px.bar(
            drop_stats,
            x='Dropout_Pct',
            y='Verzekeraar',
            orientation='h',
            text_auto='.1f',
            color='Dropout_Pct',
            color_continuous_scale='Reds',
            labels={'Dropout_Pct': 'Uitval %'},
            height=600
        )
        st.plotly_chart(fig_drop, use_container_width=True)
    else:
        st.success("Geen dropouts in de huidige selectie! 🎉")

# === TAB 2: SNELHEID ===
with tab2:
    st.subheader("Wie betaalt het snelst? (Doorlooptijd tot declarabel)")
    
    time_stats = df_zoom.groupby('Verzekeraar')['Dagen_Tot_Declarabel'].mean().reset_index()
    time_stats = time_stats.sort_values('Dagen_Tot_Declarabel', ascending=False).head(20) # Traagste bovenaan
    
    fig_time = px.bar(
        time_stats,
        x='Dagen_Tot_Declarabel',
        y='Verzekeraar',
        orientation='h',
        text_auto='.0f',
        color='Dagen_Tot_Declarabel',
        color_continuous_scale='Blues',
        labels={'Dagen_Tot_Declarabel': 'Dagen'},
        height=600,
        title="Gemiddeld aantal dagen tot declarabel (Top 20 Traagste)"
    )
    st.plotly_chart(fig_time, use_container_width=True)

    # Boxplot voor spreiding
    st.subheader("Spreiding van doorlooptijden")
    fig_box = px.box(
        df_zoom,
        x='Verzekeraar',
        y='Dagen_Tot_Declarabel',
        color='Verzekeraar',
        title="Zijn ze consistent of zijn er uitschieters?"
    )
    st.plotly_chart(fig_box, use_container_width=True)

# === TAB 3: VOLUME & WAARDE ===
with tab3:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Grootste Verzekeraars (Aantal)")
        vol_df = df_decl['Verzekeraar'].value_counts().reset_index()
        vol_df.columns = ['Verzekeraar', 'Aantal']
        
        fig_vol = px.bar(vol_df.head(15), x='Verzekeraar', y='Aantal', color='Aantal', color_continuous_scale='Viridis')
        st.plotly_chart(fig_vol, use_container_width=True)
        
    with c2:
        st.subheader("Totale Waarde (Omzet)")
        rev_df = df_decl.groupby('Verzekeraar')['Waarde_Potentieel'].sum().reset_index().sort_values('Waarde_Potentieel', ascending=False)
        
        fig_rev = px.bar(
            rev_df.head(15), 
            x='Verzekeraar', 
            y='Waarde_Potentieel',
            text_auto='.0s',
            color='Waarde_Potentieel',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig_rev, use_container_width=True)
        
    st.subheader("Data Detail")
    st.dataframe(df_decl[['deal_id', 'Verzekeraar', 'Dossier_Status', 'Waarde_Potentieel', 'createdate', 'mag_gedeclareerd_worden_datum']].sort_values('createdate', ascending=False), use_container_width=True)

# Footer
st.markdown("---")
st.caption("Gegenereerd door 'The Company' Intelligence Unit | Data is leidend.")