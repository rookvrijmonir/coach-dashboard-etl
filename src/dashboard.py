import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

# --- PAGINA CONFIGURATIE ---
st.set_page_config(page_title="Coach Benchmark & Status", layout="wide")

# Definitie van Actieve Fases (In volgorde van de pijplijn)
ACTIVE_STAGES = {
    "114855767": "Warme aanvraag",
    "15415582": "Informatie aangevraagd",
    "15415583": "Gebeld, geen gehoor",
    "15415584": "Bereikt, later terugbellen",
    "114803327": "Bereikt, belafspraak ingepland",
    "15413222": "Intake gepland",
    "15413223": "In begeleiding",
    "15413630": "Tijdelijk stoppen (50%)",
    "15413631": "Start later"
}

# Definitie van Terminale Fases
TERMINAL_STAGES = {
    "15413226": "Afgesloten / Gewonnen",
    "15413632": "Geen interesse / Verloren",
    "25956255": "Nooit bereikt"
}

# Volgorde voor weergave in tabelkolommen (Chronologisch)
PIPELINE_ORDER = list(ACTIVE_STAGES.values()) + list(TERMINAL_STAGES.values())

def load_data():
    if not os.path.exists('data/hubspot_export_raw.csv'):
        st.error("CSV niet gevonden. Draai main.py."); return None
    
    df = pd.read_csv('data/hubspot_export_raw.csv', sep=';')
    df['coach_naam'] = df['coach_naam'].astype(str).replace('nan', 'Onbekend')
    
    date_cols = ['createdate', 'ts_warme_aanvraag', 'ts_in_begeleiding', 'datum_afgesloten']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
    
    df['stage_id_str'] = df['dealstage'].astype(str)
    df['Fase'] = df['stage_id_str'].map({**ACTIVE_STAGES, **TERMINAL_STAGES}).fillna("Overig")
    df['Is_Actief'] = df['stage_id_str'].isin(ACTIVE_STAGES.keys())
    
    nu = pd.Timestamp.now()
    df['dagen_in_fase'] = (nu - df['ts_warme_aanvraag'].fillna(df['createdate'])).dt.days
    
    return df

def create_coach_summary_table(dataframe, num_coaches_denominator=1):
    """
    Maakt een samenvattingstabel waarbij de fasen de kolommen zijn.
    num_coaches_denominator wordt gebruikt voor de benchmark (totaal / aantal coaches).
    """
    # Aantallen en gemiddeldes berekenen
    counts = dataframe.groupby('Fase').size() / num_coaches_denominator
    days = dataframe.groupby('Fase')['dagen_in_fase'].mean()
    
    # DataFrame bouwen en reindexeren op pijplijn volgorde
    summary = pd.DataFrame({
        'Aantal deals': counts,
        'Gem. dagen stilstand': days
    }).reindex(PIPELINE_ORDER)
    
    # Transponeren: Fasen worden kolommen, Metrics worden rijen
    summary_t = summary.T
    return summary_t

df = load_data()

if df is not None:
    st.title("🏆 Coach Benchmark Dashboard")
    
    # --- SIDEBAR (ONGEWIJZIGD) ---
    st.sidebar.header("Instellingen")
    dagen_terug = st.sidebar.slider("Instroom periode (dagen terug):", 7, 365, 90)
    grens_datum = pd.Timestamp.now() - timedelta(days=dagen_terug)
    
    all_coaches = sorted(df['coach_naam'].unique().tolist())
    sel_coaches = st.sidebar.multiselect("Selecteer Coaches voor Vergelijking:", all_coaches, default=all_coaches[:2] if len(all_coaches)>1 else all_coaches)
    
    status_filter = st.sidebar.radio("Focus op:", ["Alleen Actieve Werkvoorraad", "Alles (inclusief Afgesloten)"])

    # --- FILTERING ---
    df_period_all = df[df['createdate'] >= grens_datum]
    
    if status_filter == "Alleen Actieve Werkvoorraad":
        df_display_all = df_period_all[df_period_all['Is_Actief'] == True]
    else:
        df_display_all = df_period_all

    # --- OVERALL METRICS ---
    st.subheader("🌐 Overall Pijplijn Status (Alle Deals)")
    overall_counts = df_display_all['Fase'].value_counts().reindex(PIPELINE_ORDER).fillna(0)
    
    fig_overall = px.bar(
        x=overall_counts.index, 
        y=overall_counts.values, 
        labels={'x': 'Fase', 'y': 'Totaal aantal deals'},
        title="Totaal aantal deals momenteel in elke fase (Overall)",
        color=overall_counts.values,
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_overall, use_container_width=True)

    st.divider()

    # --- VISUALS (GRAFIEKEN BEHOUDEN) ---
    c1, c2 = st.columns(2)
    num_total_coaches = df_period_all['coach_naam'].nunique()
    if num_total_coaches == 0: num_total_coaches = 1
    
    df_selection = df_display_all[df_display_all['coach_naam'].isin(sel_coaches)]

    with c1:
        st.subheader("Werkvoorraad vs. Benchmark")
        bench_counts = df_display_all.groupby('Fase').size() / num_total_coaches
        bench_counts = bench_counts.reset_index(name='Waarde')
        bench_counts['coach_naam'] = 'Gemiddelde (Benchmark)'
        
        plot_counts = df_selection.groupby(['coach_naam', 'Fase']).size().reset_index(name='Waarde')
        plot_counts = pd.concat([plot_counts, bench_counts])
        
        fig_hist = px.bar(
            plot_counts, x="Fase", y="Waarde", color="coach_naam", barmode="group",
            category_orders={"Fase": PIPELINE_ORDER}
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with c2:
        st.subheader("Stilstand vs. Benchmark")
        bench_days = df_display_all.groupby('Fase')['dagen_in_fase'].mean().reset_index(name='Waarde')
        bench_days['coach_naam'] = 'Gemiddelde (Benchmark)'
        
        plot_days = df_selection.groupby(['coach_naam', 'Fase'])['dagen_in_fase'].mean().reset_index(name='Waarde')
        plot_days = pd.concat([plot_days, bench_days])
        
        fig_speed = px.bar(plot_days, x="Fase", y="Waarde", color="coach_naam", barmode="group")
        fig_speed.add_hline(y=14, line_dash="dash", line_color="red", annotation_text="Stagnatiegrens")
        st.plotly_chart(fig_speed, use_container_width=True)

    st.divider()

    # --- COACH PROFIELEN (TABELLEN PER COACH + BENCHMARK) ---
    st.subheader("📋 Coach Profielen: Gedetailleerde Vergelijking")
    st.markdown("Fasen staan van links naar rechts in chronologische volgorde.")

    # 1. De Benchmark Coach
    st.write("### 🏢 Gemiddelde Coach (Organisatie Benchmark)")
    bench_table = create_coach_summary_table(df_display_all, num_coaches_denominator=num_total_coaches)
    st.table(bench_table.style.format(precision=1, na_rep="-"))
    
    st.write("---")

    # 2. Individuele Tabellen voor geselecteerde coaches
    for coach in sel_coaches:
        coach_df = df_display_all[df_display_all['coach_naam'] == coach]
        
        # We gebruiken een container om de tabel en de lijst bij elkaar te houden
        with st.container():
            st.write(f"### 👤 Coach: {coach}")
            
            # Samenvattingstabel (Precies dezelfde structuur als benchmark)
            coach_summary = create_coach_summary_table(coach_df)
            st.table(coach_summary.style.format(precision=1, na_rep="-"))
            
            # De individuele dossiers (Alleen Record ID's) in een inklapbaar menu
            with st.expander(f"Bekijk individuele dossiers (Record ID's) voor {coach}"):
                st.dataframe(
                    coach_df[['deal_id', 'Fase', 'dagen_in_fase', 'createdate']]
                    .sort_values('dagen_in_fase', ascending=False),
                    use_container_width=True,
                    hide_index=True
                )
            st.write("<br>", unsafe_allow_html=True) # Extra witruimte tussen coaches

    st.info("""
    **Uitleg Tabel:**
    * De rijen tonen het **Aantal deals** (werkdruk) en de **Gemiddelde dagen stilstand** (snelheid).
    * De kolommen volgen de pijplijn: van nieuwe aanvraag (links) tot afsluiting (rechts).
    * De **Gemiddelde Coach** is gebaseerd op de totale dataset gedeeld door het aantal actieve coaches.
    """)