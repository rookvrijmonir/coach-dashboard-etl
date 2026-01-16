import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

# --- PAGINA CONFIGURATIE ---
st.set_page_config(page_title="Coach Benchmark & Status", layout="wide")

# Definitie van Actieve Fases
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

def load_data():
    if not os.path.exists('data/hubspot_export_raw.csv'):
        st.error("CSV niet gevonden. Draai main.py."); return None
    
    df = pd.read_csv('data/hubspot_export_raw.csv', sep=';')
    # Zorg dat coach_naam altijd tekst is en vul missende waarden in
    df['coach_naam'] = df['coach_naam'].astype(str).replace('nan', 'Onbekend')
    
    # Datums converteren
    date_cols = ['createdate', 'ts_warme_aanvraag', 'ts_in_begeleiding', 'datum_afgesloten']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
    
    # Status en Actief vlag bepalen
    df['stage_id_str'] = df['dealstage'].astype(str)
    df['Fase'] = df['stage_id_str'].map({**ACTIVE_STAGES, **TERMINAL_STAGES}).fillna("Overig")
    df['Is_Actief'] = df['stage_id_str'].isin(ACTIVE_STAGES.keys())
    
    # Dagen in fase berekenen (Stagnatie)
    nu = pd.Timestamp.now()
    df['dagen_in_fase'] = (nu - df['ts_warme_aanvraag'].fillna(df['createdate'])).dt.days
    
    return df

df = load_data()

if df is not None:
    st.title("🏆 Coach Benchmark Dashboard")
    
    # --- SIDEBAR ---
    st.sidebar.header("Instellingen")
    
    # 1. Tijd (Cruciale schuifregelaar behouden)
    dagen_terug = st.sidebar.slider("Instroom periode (dagen terug):", 7, 365, 90)
    grens_datum = pd.Timestamp.now() - timedelta(days=dagen_terug)
    
    # 2. Coach Selectie
    all_coaches = sorted(df['coach_naam'].unique().tolist())
    sel_coaches = st.sidebar.multiselect("Selecteer Coaches voor Vergelijking:", all_coaches, default=all_coaches[:2] if len(all_coaches)>1 else all_coaches)
    
    # 3. Status Filter
    status_filter = st.sidebar.radio("Focus op:", 
                                    ["Alleen Actieve Werkvoorraad", "Alles (inclusief Afgesloten)"])

    # --- FILTERING ---
    # We pakken eerst de hele dataset voor de geselecteerde periode voor de Benchmark berekening
    df_period_all = df[df['createdate'] >= grens_datum]
    
    if status_filter == "Alleen Actieve Werkvoorraad":
        df_display_all = df_period_all[df_period_all['Is_Actief'] == True]
    else:
        df_display_all = df_period_all

    # Filter nu voor de geselecteerde coaches
    df_selection = df_display_all[df_display_all['coach_naam'].isin(sel_coaches)]

    # --- BENCHMARK BEREKENING ---
    # Gemiddelde werkdruk per fase (Totaal aantal deals in fase / aantal unieke coaches)
    num_coaches = df_period_all['coach_naam'].nunique()
    if num_coaches == 0: num_coaches = 1
    
    bench_counts = df_display_all.groupby('Fase').size() / num_coaches
    bench_counts = bench_counts.reset_index(name='Waarde')
    bench_counts['Type'] = 'Werkdruk (Aantal)'
    bench_counts['coach_naam'] = 'Gemiddelde (Benchmark)'

    # Gemiddelde stilstand per fase (Mean van alle deals in die fase)
    bench_days = df_display_all.groupby('Fase')['dagen_in_fase'].mean().reset_index(name='Waarde')
    bench_days['Type'] = 'Stilstand (Dagen)'
    bench_days['coach_naam'] = 'Gemiddelde (Benchmark)'

    # --- METRICS ---
    st.subheader(f"Status & Vergelijking (Sinds {grens_datum.date()})")
    m1, m2, m3, m4 = st.columns(4)
    
    with m1:
        st.metric("Geselecteerde Dossiers", len(df_selection))
    with m2:
        global_avg = df_display_all['dagen_in_fase'].mean()
        local_avg = df_selection['dagen_in_fase'].mean() if not df_selection.empty else 0
        st.metric("Gem. Dagen Stilstand", f"{local_avg:.1f} dgn", 
                  delta=f"{local_avg - global_avg:.1f} vs Gem." if local_avg > 0 else None, 
                  delta_color="inverse")
    with m3:
        actieve_procent = (len(df_selection[df_selection['Is_Actief']]) / len(df_selection) * 100) if not df_selection.empty else 0
        st.metric("Percentage Actief", f"{actieve_procent:.0f}%")
    with m4:
        st.metric("Aantal Coaches in Systeem", num_coaches)

    st.divider()

    # --- VISUALS ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Werkvoorraad vs. Benchmark")
        # Combineer geselecteerde data met benchmark data voor de grafiek
        plot_counts = df_selection.groupby(['coach_naam', 'Fase']).size().reset_index(name='Waarde')
        # Voeg benchmark toe
        plot_counts = pd.concat([plot_counts, bench_counts.rename(columns={'Waarde': 'Waarde'})])
        
        fig_hist = px.bar(
            plot_counts, 
            x="Fase", 
            y="Waarde", 
            color="coach_naam", 
            barmode="group",
            title="Aantal dossiers per fase (Gemiddelde vs Geselecteerd)",
            category_orders={"Fase": list(ACTIVE_STAGES.values()) + list(TERMINAL_STAGES.values())}
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with c2:
        st.subheader("Stilstand vs. Benchmark")
        # Combineer geselecteerde data met benchmark data
        plot_days = df_selection.groupby(['coach_naam', 'Fase'])['dagen_in_fase'].mean().reset_index(name='Waarde')
        plot_days = pd.concat([plot_days, bench_days])
        
        fig_speed = px.bar(
            plot_days, 
            x="Fase", 
            y="Waarde", 
            color="coach_naam", 
            barmode="group",
            title="Gemiddelde dagen stilstand (Gemiddelde vs Geselecteerd)"
        )
        fig_speed.add_hline(y=14, line_dash="dash", line_color="red", annotation_text="Stagnatiegrens")
        st.plotly_chart(fig_speed, use_container_width=True)

    # --- PRIVACY-VEILIGE TABEL ---
    st.subheader("Dossier Details (Zonder Namen)")
    # We tonen hier de records per geselecteerde coach
    # Door record-id te gebruiken ipv naam is de tabel AVG-veilig
    cols_to_show = ['deal_id', 'coach_naam', 'Fase', 'dagen_in_fase', 'createdate']
    
    st.dataframe(
        df_selection[cols_to_show].sort_values(['coach_naam', 'dagen_in_fase'], ascending=[True, False]),
        use_container_width=True,
        hide_index=True
    )

    st.info("""
    **Uitleg Benchmark:**
    * De groep **Gemiddelde (Benchmark)** berekent voor elke fase hoeveel dossiers een coach *gemiddeld* heeft over het hele systeem.
    * Als een coach veel hoger scoort dan de benchmark in een actieve fase, kan dit duiden op een te hoge werkdruk.
    * In de stilstand grafiek zie je of de coach sneller of langzamer werkt dan het organisatie-gemiddelde.
    """)