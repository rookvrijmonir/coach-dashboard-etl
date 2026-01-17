import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- PAGINA CONFIGURATIE ---
st.set_page_config(page_title="Regie Center: Gebiedsanalyse", layout="wide")

# CUSTOM STYLING (Licht, modern, focus op leesbaarheid)
st.markdown("""
    <style>
    .main { background-color: #fcfcfc; }
    h1, h2, h3 { color: #1a1a1a; font-family: 'Helvetica Neue', sans-serif; font-weight: 800; }
    .stMetric { border: 1px solid #eee; padding: 15px; border-radius: 10px; background-color: #ffffff; }
    .chart-container { padding: 20px; border: 1px solid #f0f0f0; border-radius: 10px; background-color: #ffffff; margin-bottom: 20px; }
    .chart-title { font-size: 1.4rem; font-weight: bold; margin-bottom: 15px; color: #1a1a1a; text-transform: uppercase; border-left: 5px solid #2c3e50; padding-left: 10px; }
    </style>
    """, unsafe_allow_html=True)

# CONFIGURATIE FASES
ACTIEVE_STAGES = ["114855767", "15415582", "15415583", "15415584", "114803327", "15413222", "15413223", "15413630", "15413631"]
GEWONNEN_STAGES = ["15413226"]
VERLOREN_STAGES = ["15413632", "25956255"]

@st.cache_data
def load_all_data():
    if not os.path.exists('data/hubspot_export_raw.csv') or not os.path.exists('data/postcodevlakken.csv'):
        return None, None, None
    
    # 1. HubSpot Data
    hs = pd.read_csv('data/hubspot_export_raw.csv', sep=';')
    hs['createdate'] = pd.to_datetime(hs['createdate'], errors='coerce', utc=True).dt.tz_localize(None)
    hs['pc4'] = hs['postcode'].astype(str).str.extract(r'(\d{4})')[0]
    
    # 2. Postcodevlakken
    pv = pd.read_csv('data/postcodevlakken.csv', sep=None, engine='python')
    pv.columns = [c.replace(';', '').strip() for c in pv.columns]
    pv = pv.loc[:, ~pv.columns.duplicated()]
    pv['Postcode_Match'] = pv['Postcodevlak'].astype(str).str.extract(r'(\d{4})')[0]
    
    # Lat/Long fix
    for col in ['Lat', 'Long']:
        if col in pv.columns:
            pv[col] = pv[col].astype(str).str.replace(',', '.').str.replace(';', '').str.strip()
            pv[col] = pd.to_numeric(pv[col], errors='coerce')
    pv = pv.dropna(subset=['Lat', 'Long'])

    # 3. Pakketnamen inladen uit nieuwe CSV
    pn_map = {}
    if os.path.exists('data/pakketnamen.csv'):
        pn = pd.read_csv('data/pakketnamen.csv', sep=None, engine='python')
        # We verwachten: [Pakketnaam], [Coach]
        for _, row in pn.iterrows():
            name = str(row.iloc[0]).strip()
            pn_map[name] = name # Simpele mapping voor lookup
    
    return hs, pv, pn_map

# --- DATA LADEN ---
hs_df, pv_df, pn_map = load_all_data()

if hs_df is not None and pv_df is not None:
    # --- SIDEBAR REGIE ---
    st.sidebar.title("🎛️ Regie Paneel")
    
    if st.sidebar.button("🔄 Reset naar Totaal"):
        st.session_state['selected_coach'] = "TOTAAL OVERZICHT"
        st.rerun()

    # 1. Datum Filter
    min_date = hs_df['createdate'].min().date()
    max_date = hs_df['createdate'].max().date()
    start_d, end_d = st.sidebar.slider("Instroom periode:", min_date, max_date, (min_date, max_date))

    # 2. Lead Status Filter
    st.sidebar.subheader("📊 Lead Status")
    show_actief = st.sidebar.checkbox("Actieve Leads", value=True)
    show_gewonnen = st.sidebar.checkbox("Gewonnen (Kassa)", value=True)
    show_verloren = st.sidebar.checkbox("Verloren", value=False)

    # 3. Coach Selectie
    st.sidebar.subheader("👤 Coach")
    all_coaches = sorted(list(set(pv_df['Coach'].dropna().tolist() + pv_df['Overnemende Coach'].dropna().tolist())))
    selected_coach = st.sidebar.selectbox("Kies een coach:", ["TOTAAL OVERZICHT"] + all_coaches, key='selected_coach')
    
    # 4. Top X regelaar
    top_x = st.sidebar.slider("Aantal gebieden in de grafieken:", 5, 50, 10)

    # --- DATA FILTERING ---
    # Tijd & Status filter
    status_list = []
    if show_actief: status_list += ACTIEVE_STAGES
    if show_gewonnen: status_list += GEWONNEN_STAGES
    if show_verloren: status_list += VERLOREN_STAGES
    
    hs_f = hs_df[(hs_df['createdate'].dt.date >= start_d) & (hs_df['createdate'].dt.date <= end_d) & (hs_df['dealstage'].astype(str).isin(status_list))].copy()

    # Koppelen Leads aan Postcodes
    counts = hs_f.groupby('pc4').size().reset_index(name='Leads')
    map_data = pd.merge(pv_df, counts, left_on='Postcode_Match', right_on='pc4', how='left').fillna({'Leads': 0})
    map_data['Leads'] = map_data['Leads'].astype(int)

    # --- UI ---
    st.title("⚖️ GEOGRAFISCH COMMAND CENTER")
    
    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    col_kpi1.metric("Geselecteerde Leads", f"{int(map_data['Leads'].sum())}")
    col_kpi2.metric("Postcodes met Activiteit", len(map_data[map_data['Leads'] > 0]))
    col_kpi3.metric("Selectie", selected_coach)

    st.divider()

    # --- KAART ---
    st.header(f"📍 Gebiedskaart: {selected_coach}")
    
    # Voor de kaart bepalen we de categorieën op basis van de geselecteerde coach
    def get_map_category(row):
        if selected_coach == "TOTAAL OVERZICHT": return "Totaal"
        if str(row['Coach']) == selected_coach: return "Focus"
        if str(row['BonusCoach']) == selected_coach: return "Bonus"
        if str(row['Resterende Coach']) == selected_coach: return "Resterend"
        if str(row['Overnemende Coach']) == selected_coach: return "Overname"
        return "Overig"

    map_data['KaartCategorie'] = map_data.apply(get_map_category, axis=1)
    
    # Kaart plotten
    plot_data = map_data[map_data['KaartCategorie'] != "Overig"] if selected_coach != "TOTAAL OVERZICHT" else map_data[map_data['Leads'] > 0]
    
    fig_map = px.scatter_mapbox(
        plot_data, lat="Lat", lon="Long", color="KaartCategorie" if selected_coach != "TOTAAL OVERZICHT" else None,
        size=plot_data['Leads'] + 2,
        hover_name="Woonplaats",
        hover_data={"Leads": True, "Postcode_Match": True, "Coach": True},
        zoom=6.8, center=dict(lat=52.2, lon=5.3),
        mapbox_style="carto-positron", height=600
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

    st.divider()

    # --- DE 3 SEPARATE GRAFIEKEN ---
    if selected_coach == "TOTAAL OVERZICHT":
        st.header(f"📈 Top {top_x} Gebieden per Categorie")
        
        # 1. FOCUS GEBIEDEN PLOT
        # We groeperen alle postcodes op de VASTE coach (niet overnemend)
        focus_df = map_data[map_data['Coach'].notnull()].groupby('Coach')['Leads'].sum().reset_index()
        focus_df['Label'] = "Focus - " + focus_df['Coach']
        focus_df = focus_df.sort_values('Leads', ascending=False).head(top_x)
        
        with st.container():
            st.markdown("<div class='chart-title'>Top Focusgebieden</div>", unsafe_allow_html=True)
            if not focus_df.empty:
                fig_f = px.bar(focus_df, x='Leads', y='Label', orientation='h', 
                               color='Leads', color_continuous_scale='Blues',
                               text='Leads', labels={'Leads': 'Aantal Leads', 'Label': 'Focus Gebied'})
                fig_f.update_layout(yaxis={'categoryorder':'total ascending'}, coloraxis_showscale=False, height=400)
                fig_f.update_xaxes(tickformat=',d')
                st.plotly_chart(fig_f, use_container_width=True)
            else: st.write("Geen data")

        # 2. BONUS GEBIEDEN PLOT
        bonus_df = map_data[map_data['BonusCoach'].notnull()].groupby('BonusCoach')['Leads'].sum().reset_index()
        bonus_df['Label'] = "Bonus - " + bonus_df['BonusCoach']
        bonus_df = bonus_df.sort_values('Leads', ascending=False).head(top_x)

        with st.container():
            st.markdown("<div class='chart-title'>Top Bonusgebieden</div>", unsafe_allow_html=True)
            if not bonus_df.empty:
                fig_b = px.bar(bonus_df, x='Leads', y='Label', orientation='h', 
                               color='Leads', color_continuous_scale='Oranges',
                               text='Leads', labels={'Leads': 'Aantal Leads', 'Label': 'Bonus Gebied'})
                fig_b.update_layout(yaxis={'categoryorder':'total ascending'}, coloraxis_showscale=False, height=400)
                fig_b.update_xaxes(tickformat=',d')
                st.plotly_chart(fig_b, use_container_width=True)
            else: st.write("Geen data")

        # 3. RESTERENDE GEBIEDEN PLOT
        resterend_df = map_data[map_data['Resterend Pakket'].notnull()].groupby('Resterend Pakket')['Leads'].sum().reset_index()
        resterend_df['Label'] = resterend_df['Resterend Pakket'].apply(lambda x: pn_map.get(str(x), str(x)))
        resterend_df = resterend_df.sort_values('Leads', ascending=False).head(top_x)

        with st.container():
            st.markdown("<div class='chart-title'>Top Resterende Gebieden (Pakketten)</div>", unsafe_allow_html=True)
            if not resterend_df.empty:
                fig_r = px.bar(resterend_df, x='Leads', y='Label', orientation='h', 
                               color='Leads', color_continuous_scale='Greens',
                               text='Leads', labels={'Leads': 'Aantal Leads', 'Label': 'Pakketnaam'})
                fig_r.update_layout(yaxis={'categoryorder':'total ascending'}, coloraxis_showscale=False, height=400)
                fig_r.update_xaxes(tickformat=',d')
                st.plotly_chart(fig_r, use_container_width=True)
            else: st.write("Geen data")

    else:
        # Weergave voor individuele coach (Detail)
        st.header(f"Detailanalyse: {selected_coach}")
        df_coach = map_data[map_data['KaartCategorie'] != "Overig"].groupby(['KaartCategorie', 'Postcode_Match'])['Leads'].sum().reset_index()
        fig_coach = px.bar(df_coach, x='Leads', y='Postcode_Match', color='KaartCategorie', barmode='group',
                          text='Leads', orientation='h', title=f"Leads per Postcode voor {selected_coach}")
        fig_coach.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
        st.plotly_chart(fig_coach, use_container_width=True)

else:
    st.error("Bestanden niet gevonden in 'data/'.")