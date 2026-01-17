import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

# --- PAGINA CONFIGURATIE ---
st.set_page_config(page_title="Coach Benchmark & Status", layout="wide")

# Definitie van Actieve FASES (In de werkelijke chronologische volgorde)
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
    "25956255": "Geen interesse / Verloren"
}

# DE UNIFORME VOLGORDE (Linker volgorde / Chronologisch)
PIPELINE_ORDER = [
    "Warme aanvraag", 
    "Informatie aangevraagd", 
    "Gebeld, geen gehoor", 
    "Bereikt, later terugbellen", 
    "Bereikt, belafspraak ingepland", 
    "Intake gepland", 
    "In begeleiding", 
    "Tijdelijk stoppen (50%)", 
    "Start later", 
    "Afgesloten / Gewonnen", 
    "Geen interesse / Verloren"
]

def load_data():
    if not os.path.exists('data/hubspot_export_raw.csv'):
        st.error("CSV niet gevonden. Draai main.py."); return None
    
    df = pd.read_csv('data/hubspot_export_raw.csv', sep=';')
    df['coach_naam'] = df['coach_naam'].astype(str).replace('nan', 'Onbekend')
    
    date_cols = ['createdate', 'ts_warme_aanvraag', 'ts_in_begeleiding', 'datum_afgesloten']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
    
    df['stage_id_str'] = df['dealstage'].astype(str)
    
    # 1. Bepaal of een deal ACTIEF is
    df['Is_Actief'] = df['stage_id_str'].isin(ACTIVE_STAGES.keys())
    
    # 2. Mappen van fases
    all_maps = {**ACTIVE_STAGES, **TERMINAL_STAGES}
    df['Fase'] = df['stage_id_str'].map(all_maps)
    
    # 3. Logica voor 'Overig' (niet gemapped en niet actief wordt als verloren geteld)
    df.loc[df['Fase'].isna() & ~df['Is_Actief'], 'Fase'] = "Geen interesse / Verloren"
    df['Fase'] = df['Fase'].fillna("Overig")
    
    # 4. Markeer definitieve uitkomst voor de Succesratio
    df['Is_Gewonnen'] = (df['Fase'] == "Afgesloten / Gewonnen")
    df['Is_Verloren'] = (df['Fase'] == "Geen interesse / Verloren")
    
    nu = pd.Timestamp.now()
    df['dagen_in_fase'] = (nu - df['ts_warme_aanvraag'].fillna(df['createdate'])).dt.days
    
    return df

def create_coach_summary_table(dataframe, num_coaches_denominator=1):
    """
    Maakt een samenvattingstabel waarbij de fasen de kolommen zijn.
    """
    counts = (dataframe.groupby('Fase').size() / num_coaches_denominator).round(1)
    days = dataframe.groupby('Fase')['dagen_in_fase'].mean().round(1)
    
    total = counts.sum()
    percentages = ((counts / total) * 100).round(1) if total > 0 else 0
    
    summary = pd.DataFrame({
        'Aantal deals': counts,
        'Percentage (%)': percentages,
        'Gem. dagen stilstand': days
    }).reindex(PIPELINE_ORDER)
    
    return summary.T

df = load_data()

if df is not None:
    st.title("🏆 Coach Benchmark Dashboard")
    
    # --- SIDEBAR ---
    st.sidebar.header("Instellingen")
    
    # Slider voor instroom (per dag, vanaf 1)
    dagen_terug = st.sidebar.slider("Instroom periode (dagen terug):", min_value=1, max_value=365, value=90, step=1)
    grens_datum = pd.Timestamp.now() - timedelta(days=dagen_terug)
    
    all_coaches = sorted(df['coach_naam'].unique().tolist())
    sel_coaches = st.sidebar.multiselect("Selecteer Coaches voor Vergelijking:", all_coaches, default=all_coaches[:2] if len(all_coaches)>1 else all_coaches)
    
    status_filter = st.sidebar.radio("Welke dossiers wil je zien?", ["Alles (Pijplijn + Afgesloten)", "Alleen Actieve Werkvoorraad"])

    # --- FILTERING ---
    df_period_all = df[df['createdate'] >= grens_datum]
    
    if status_filter == "Alleen Actieve Werkvoorraad":
        df_display_all = df_period_all[df_period_all['Is_Actief'] == True]
    else:
        df_display_all = df_period_all

    # --- TOP LEVEL OVERALL METRICS ---
    st.info(f"📊 **Overzicht:** Er zijn in totaal **{len(df_period_all)}** dossiers geselecteerd in de periode vanaf {grens_datum.date()}.")

    # --- OVERALL PIJPLIJN STATUS ---
    st.subheader("🌐 Overall Pijplijn Status (Alle Coaches)")
    overall_counts = df_display_all['Fase'].value_counts().reindex(PIPELINE_ORDER).fillna(0)
    total_in_view = overall_counts.sum()
    
    text_labels = [f"{int(c)} ({(c/total_in_view*100):.1f}%)" if total_in_view > 0 else "0" for c in overall_counts.values]

    fig_overall = px.bar(
        x=overall_counts.index, 
        y=overall_counts.values, 
        text=text_labels,
        labels={'x': 'Fase', 'y': 'Aantal deals'},
        title=f"Verdeling van alle dossiers over de fasen (Uniforme Volgorde)",
        color=overall_counts.values,
        color_continuous_scale='Blues',
        category_orders={"x": PIPELINE_ORDER} # Harde afdwinging van de volgorde
    )
    fig_overall.update_traces(textposition='outside')
    st.plotly_chart(fig_overall, use_container_width=True)

    st.divider()

    # --- VISUALS ---
    c1, c2 = st.columns(2)
    num_total_coaches = df_period_all['coach_naam'].nunique() or 1
    df_selection = df_display_all[df_display_all['coach_naam'].isin(sel_coaches)]

    with c1:
        st.subheader("Werkvoorraad vs. Benchmark")
        bench_counts = (df_display_all.groupby('Fase').size() / num_total_coaches).reset_index(name='Waarde')
        bench_counts['Waarde'] = bench_counts['Waarde'].round(1)
        bench_counts['coach_naam'] = 'Gemiddelde (Benchmark)'
        
        sel_counts = df_selection.groupby(['coach_naam', 'Fase']).size().reset_index(name='Waarde')
        plot_counts = pd.concat([sel_counts, bench_counts])
        
        fig_hist = px.bar(
            plot_counts, x="Fase", y="Waarde", color="coach_naam", barmode="group",
            category_orders={"Fase": PIPELINE_ORDER},
            title="Aantal dossiers: Vergelijking werkdruk"
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with c2:
        st.subheader("Stilstand vs. Benchmark")
        bench_days = df_display_all.groupby('Fase')['dagen_in_fase'].mean().reset_index(name='Waarde')
        bench_days['Waarde'] = bench_days['Waarde'].round(1)
        bench_days['coach_naam'] = 'Gemiddelde (Benchmark)'
        
        sel_days = df_selection.groupby(['coach_naam', 'Fase'])['dagen_in_fase'].mean().reset_index(name='Waarde')
        sel_days['Waarde'] = sel_days['Waarde'].round(1)
        plot_days = pd.concat([sel_days, bench_days])
        
        fig_speed = px.bar(
            plot_days, x="Fase", y="Waarde", color="coach_naam", barmode="group",
            category_orders={"Fase": PIPELINE_ORDER},
            title="Gemiddelde dagen stilstand per fase"
        )
        st.plotly_chart(fig_speed, use_container_width=True)

    st.divider()

    # --- COACH PROFIELEN ---
    st.subheader("📋 Coach Profielen: Succesratio & Werkdruk")

    # 1. Benchmark
    with st.container():
        st.write("### 🏢 Gemiddelde Coach (Organisatie Benchmark)")
        
        # Succesratio benchmark
        b_won_raw = len(df_period_all[df_period_all['Is_Gewonnen']])
        b_lost_raw = len(df_period_all[df_period_all['Is_Verloren']])
        b_succes_ratio = round((b_won_raw / (b_won_raw + b_lost_raw) * 100), 1) if (b_won_raw + b_lost_raw) > 0 else 0
        
        col_b1, col_b2 = st.columns([1, 4])
        col_b1.metric("Succesratio (Benchmark)", f"{b_succes_ratio}%")
        
        bench_table = create_coach_summary_table(df_display_all, num_coaches_denominator=num_total_coaches)
        st.table(bench_table.style.format(precision=1, na_rep="-"))
    
    st.write("---")

    # 2. Individuele Coaches
    for coach in sel_coaches:
        coach_all = df_period_all[df_period_all['coach_naam'] == coach]
        coach_display = df_display_all[df_display_all['coach_naam'] == coach]
        
        with st.container():
            st.write(f"### 👤 Coach: {coach}")
            
            # SUCCESRATIO BEREKENING (Alleen G vs V)
            c_won = len(coach_all[coach_all['Is_Gewonnen']])
            c_lost = len(coach_all[coach_all['Is_Verloren']])
            c_succes_ratio = round((c_won / (c_won + c_lost) * 100), 1) if (c_won + c_lost) > 0 else 0
            
            diff = round(c_succes_ratio - b_succes_ratio, 1)
            
            col1, col2, col3 = st.columns([1, 1, 3])
            col1.metric("Succesratio (G/V)", f"{c_succes_ratio}%", delta=f"{diff}% vs Gem.")
            
            col2.metric("Werkvoorraad", len(coach_display[coach_display['Is_Actief']]))
            
            coach_summary = create_coach_summary_table(coach_display)
            st.table(coach_summary.style.format(precision=1, na_rep="-"))
            
            with st.expander(f"Bekijk dossier details voor {coach}"):
                st.dataframe(
                    coach_display[['deal_id', 'Fase', 'dagen_in_fase', 'createdate']]
                    .sort_values('dagen_in_fase', ascending=False),
                    use_container_width=True,
                    hide_index=True
                )
            st.write("<br>", unsafe_allow_html=True)

    st.info("""
    **Legenda & Uniformiteit:**
    * De fasen staan overal van links naar rechts in de werkelijke procesvolgorde.
    * Alle decimalen zijn consequent afgerond op **1 cijfer** achter de komma.
    * **Succesratio (G/V):** Conversie gebaseerd op dossiers die een eindstation hebben bereikt.
    """)