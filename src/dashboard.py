import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

# --- CONFIGURATIE ---
st.set_page_config(page_title="Coach Benchmark & Efficiency", layout="wide")

# DEFINITIES VAN VERZEKERAARSGROEPEN
VGZ_GROUP = ['vgz', 'unive', 'univé', 'bewuzt', 'izz', 'iza', 'umc', 'zekur', 'ku']
DSW_GROUP = ['dsw', 'stad holland', 'intwente', 'rma', 'rmo', 'svzk']

# FASES
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

TERMINAL_STAGES = {
    "15413226": "Afgesloten / Gewonnen",
    "15413632": "Geen interesse / Verloren",
    "25956255": "Geen interesse / Verloren"
}

PIPELINE_ORDER = [
    "Warme aanvraag", "Informatie aangevraagd", "Gebeld, geen gehoor", 
    "Bereikt, later terugbellen", "Bereikt, belafspraak ingepland", 
    "Intake gepland", "In begeleiding", "Tijdelijk stoppen (50%)", 
    "Start later", "Afgesloten / Gewonnen", "Geen interesse / Verloren"
]

def load_data():
    if not os.path.exists('data/hubspot_export_raw.csv'):
        st.error("CSV niet gevonden. Draai main.py."); return None
    
    df = pd.read_csv('data/hubspot_export_raw.csv', sep=';')
    df['coach_naam'] = df['coach_naam'].astype(str).replace('nan', 'Onbekend')
    
    date_cols = ['createdate', 'ts_warme_aanvraag', 'ts_in_begeleiding', 'datum_afgesloten', 'datum_ig']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
    
    df['stage_id_str'] = df['dealstage'].astype(str)
    df['Is_Actief'] = df['stage_id_str'].isin(ACTIVE_STAGES.keys())
    
    all_maps = {**ACTIVE_STAGES, **TERMINAL_STAGES}
    df['Fase'] = df['stage_id_str'].map(all_maps)
    
    df.loc[df['Fase'].isna() & ~df['Is_Actief'], 'Fase'] = "Geen interesse / Verloren"
    df['Fase'] = df['Fase'].fillna("Overig")
    
    nu = pd.Timestamp.now()
    df['dagen_in_fase'] = (nu - df['ts_warme_aanvraag'].fillna(df['createdate'])).dt.days
    
    # --- DECLARATIE LOGICA ---
    def determine_declarable_status(row):
        verz = str(row['verzekeraar']).lower()
        begeleiding = str(row['begeleiding'])
        is_won = row['stage_id_str'] == "15413226"
        
        dec_date = pd.NaT
        if any(v in verz for v in VGZ_GROUP):
            if row['vgz_voldoende'] == 'Ja' and pd.notnull(row['datum_afgesloten']):
                if row['datum_afgesloten'] > pd.Timestamp('2024-11-01'): dec_date = row['datum_afgesloten']
        elif any(d in verz for d in DSW_GROUP):
            if row['dsw_sessie'] == 'Ja' and pd.notnull(row['ts_in_begeleiding']):
                if row['ts_in_begeleiding'] >= pd.Timestamp('2024-08-01'): dec_date = row['ts_in_begeleiding']
        elif pd.notnull(row['verzekeraar']) and row['verzekeraar'] != 'nan':
            if pd.notnull(row['ts_in_begeleiding']):
                if row['ts_in_begeleiding'] > pd.Timestamp('2024-11-01'): dec_date = row['ts_in_begeleiding']

        if pd.isnull(dec_date): return pd.NaT, "Niet declarabel"
        if is_won:
            is_dropout = ("Alleen de intake" in begeleiding) or ("minder dan de helft" in begeleiding)
            return dec_date, "Dropout" if is_dropout else "Volledig"
        return dec_date, "Declarabel (In proces)"

    df[['datum_declarabel', 'Declaratie_Type']] = df.apply(lambda r: pd.Series(determine_declarable_status(r)), axis=1)
    df['dagen_tot_geld'] = (df['datum_declarabel'] - df['createdate']).dt.days

    # Conversie (incl 30-dagen regel)
    df['Conv_Gewonnen'] = (df['stage_id_str'] == "15413226")
    df['Conv_Verloren'] = ((df['Fase'] == "Geen interesse / Verloren") | ((df['stage_id_str'] == "15415583") & (df['dagen_in_fase'] > 30)))
    
    return df

def styled_table(df_summary):
    """Modern hoog-contrast styling voor maximale leesbaarheid."""
    return df_summary.style.format(precision=1, na_rep="-")\
        .set_properties(**{
            'text-align': 'center', 
            'font-family': 'Arial, sans-serif',
            'font-size': '15px',
            'color': '#000000',
            'border': '1px solid #e0e0e0',
            'background-color': '#ffffff'
        })\
        .set_table_styles([
            {'selector': 'th', 'props': [
                ('background-color', '#2c3e50'), 
                ('color', 'white'), 
                ('font-weight', 'bold'), 
                ('text-transform', 'uppercase'),
                ('padding', '10px')
            ]},
            {'selector': 'td:hover', 'props': [('background-color', '#f5f5f5')]}
        ]).pipe(lambda s: s.data.T)

def create_summary(dataframe, denom=1):
    counts = (dataframe.groupby('Fase').size() / denom).round(1)
    days = dataframe.groupby('Fase')['dagen_in_fase'].mean().round(1)
    total = counts.sum()
    pct = ((counts / total) * 100).round(1) if total > 0 else 0
    return pd.DataFrame({'Aantal deals': counts, 'Percentage (%)': pct, 'Gem. dagen stilstand': days}).reindex(PIPELINE_ORDER)

# --- NAVIGATION & UI ---
st.sidebar.title("🎛️ Dashboard Navigatie")
app_mode = st.sidebar.selectbox("Kies Scherm:", ["Coach Benchmark & Efficiency", "Financiële Forecast (Coming Soon)", "Regionale Analyse (Coming Soon)"])

df = load_data()

if df is not None:
    if app_mode == "Coach Benchmark & Efficiency":
        st.title("🏆 Coach Benchmark & Efficiency")
        
        # --- SIDEBAR FILTERS ---
        st.sidebar.divider()
        st.sidebar.header("Filter Instellingen")
        dagen_terug = st.sidebar.slider("Instroom periode (dagen):", 1, 365, 90)
        grens_datum = pd.Timestamp.now() - timedelta(days=dagen_terug)
        all_coaches = sorted(df['coach_naam'].unique().tolist())
        sel_coaches = st.sidebar.multiselect("Selecteer Coaches:", all_coaches, default=all_coaches[:2] if len(all_coaches)>1 else all_coaches)
        view_mode = st.sidebar.radio("Systeem Focus:", ["Alleen Actieve Werkvoorraad", "Alles (Historie: Won/Lost)"])

        df_period = df[df['createdate'] >= grens_datum]
        if view_mode == "Alleen Actieve Werkvoorraad":
            df_display = df_period[df_period['Is_Actief']]
            info_txt = "Actieve dossiers in de pijplijn"
        else:
            df_display = df_period[~df_period['Is_Actief']]
            info_txt = "Afgesloten resultaten (Won/Lost)"

        st.info(f"📊 **Huidige Selectie:** {info_txt} | Totaal dossiers: **{len(df_display)}**")

        # --- 1. OVERALL STATUS ---
        st.subheader("🌐 Pijplijn Verdeling (Alle Coaches)")
        counts = df_display['Fase'].value_counts().reindex(PIPELINE_ORDER).fillna(0)
        total = counts.sum()
        # Waarden en percentages voor de staven
        labels = [f"{int(v)} ({(v/total*100 if total>0 else 0):.1f}%)" for v in counts.values]
        fig_overall = px.bar(x=counts.index, y=counts.values, text=labels, color=counts.values, color_continuous_scale='Blues')
        fig_overall.update_traces(textposition='outside')
        st.plotly_chart(fig_overall, use_container_width=True)

        st.divider()

        # --- 2. EFFICIENCY ---
        st.subheader("⏱️ Efficiency: Doorlooptijd tot Declaratie")
        eff_df = df_period[df_period['dagen_tot_geld'].notna()]
        if not eff_df.empty:
            target_eff = eff_df[~eff_df['Is_Actief']] if view_mode == "Alles (Historie: Won/Lost)" else eff_df[eff_df['Is_Actief']]
            if not target_eff.empty:
                avg_eff = target_eff.groupby(['coach_naam', 'Declaratie_Type'])['dagen_tot_geld'].mean().reset_index()
                for dtype in target_eff['Declaratie_Type'].unique():
                    b_val = target_eff[target_eff['Declaratie_Type'] == dtype]['dagen_tot_geld'].mean()
                    avg_eff.loc[len(avg_eff)] = ['Gemiddelde (Benchmark)', dtype, b_val]
                
                plot_eff = avg_eff[avg_eff['coach_naam'].isin(sel_coaches + ['Gemiddelde (Benchmark)'])]
                # Voeg labels toe aan Efficiency grafiek
                fig_eff = px.bar(plot_eff, x='coach_naam', y='dagen_tot_geld', color='Declaratie_Type', 
                                 barmode='group', text=plot_eff['dagen_tot_geld'].round(1),
                                 color_discrete_map={'Volledig': '#2c3e50', 'Dropout': '#e67e22', 'Declarabel (In proces)': '#3498db'})
                fig_eff.update_traces(textposition='outside')
                st.plotly_chart(fig_eff, use_container_width=True)

        st.divider()

        # --- 3. WERKDRUK & STILSTAND ---
        st.subheader("📊 Werkdruk & Procesgezondheid")
        c1, c2 = st.columns(2)
        num_total_coaches = df_period['coach_naam'].nunique() or 1
        df_selection = df_display[df_display['coach_naam'].isin(sel_coaches)]
        
        with c1:
            st.write("#### Volume vs. Benchmark")
            bc = (df_display.groupby('Fase').size() / num_total_coaches).reset_index(name='Waarde').round(1)
            bc['coach_naam'] = 'Gemiddelde (Benchmark)'
            sc = df_selection.groupby(['coach_naam', 'Fase']).size().reset_index(name='Waarde').round(1)
            plot_c = pd.concat([sc, bc])
            fig_w = px.bar(plot_c, x="Fase", y="Waarde", color="coach_naam", barmode="group", category_orders={"Fase": PIPELINE_ORDER}, text='Waarde')
            fig_w.update_traces(textposition='outside')
            st.plotly_chart(fig_w, use_container_width=True)
        
        with c2:
            st.write("#### Stilstand (Dagen) vs. Benchmark")
            bd = df_display.groupby('Fase')['dagen_in_fase'].mean().reset_index(name='Waarde').round(1)
            bd['coach_naam'] = 'Gemiddelde (Benchmark)'
            sd = df_selection.groupby(['coach_naam', 'Fase'])['dagen_in_fase'].mean().reset_index(name='Waarde').round(1)
            plot_d = pd.concat([sd, bd])
            fig_s = px.bar(plot_d, x="Fase", y="Waarde", color="coach_naam", barmode="group", category_orders={"Fase": PIPELINE_ORDER}, text='Waarde')
            fig_s.update_traces(textposition='outside')
            st.plotly_chart(fig_s, use_container_width=True)

        st.divider()

        # --- 4. CONVERSIE ---
        st.subheader("🎯 Conversie Analyse (Won vs. Lost)")
        def build_conv_df(data, label):
            w, l = data['Conv_Gewonnen'].sum(), data['Conv_Verloren'].sum()
            tot = w + l
            if tot == 0: return None
            return [{'Coach': label, 'Type': 'Gewonnen', 'Aantal': round(float(w), 1), 'Percentage': round((w/tot*100), 1)},
                    {'Coach': label, 'Type': 'Verloren', 'Aantal': round(float(l), 1), 'Percentage': round((l/tot*100), 1)}]
        
        conv_plot_data = []
        bench_data = build_conv_df(df_period, "Gemiddelde (Benchmark)")
        if bench_data:
            for item in bench_data: item['Aantal'] = round(item['Aantal'] / num_total_coaches, 1)
            conv_plot_data.extend(bench_data)
        for cn in sel_coaches:
            cr = build_conv_df(df_period[df_period['coach_naam'] == cn], cn)
            if cr: conv_plot_data.extend(cr)
        
        if conv_plot_data:
            df_c = pd.DataFrame(conv_plot_data)
            df_c['Label'] = df_c.apply(lambda r: f"{r['Aantal']} ({r['Percentage']}%)", axis=1)
            fig_conv = px.bar(df_c, x="Coach", y="Aantal", color="Type", text="Label", barmode="group", color_discrete_map={'Gewonnen': '#27ae60', 'Verloren': '#c0392b'})
            fig_conv.update_traces(textposition='outside')
            st.plotly_chart(fig_conv, use_container_width=True)

        st.divider()

        # --- 5. PERFORMANCE PROFIELEN ---
        st.subheader("📋 Performance Profielen (Detailoverzicht)")
        with st.expander("🏢 ORGANISATIE GEMIDDELDE (Benchmark)", expanded=True):
            st.table(styled_table(create_summary(df_display, num_total_coaches)))

        for coach in sel_coaches:
            c_df = df_display[df_display['coach_naam'] == coach]
            c_all = df_period[df_period['coach_naam'] == coach]
            with st.container():
                st.markdown(f"#### 👤 Coach: **{coach}**")
                w, l = c_all['Conv_Gewonnen'].sum(), c_all['Conv_Verloren'].sum()
                ratio = round((w/(w+l)*100),1) if (w+l)>0 else 0
                col_m, col_t = st.columns([1, 4])
                col_m.metric("Succesratio (G/V)", f"{ratio}%")
                col_t.table(styled_table(create_summary(c_df)))
                with st.expander(f"Dossier details voor {coach}"):
                    st.dataframe(c_df[['deal_id', 'Fase', 'dagen_in_fase', 'createdate']].sort_values('dagen_in_fase', ascending=False), use_container_width=True, hide_index=True)
                st.write("<br>", unsafe_allow_html=True)
    
    else:
        st.title(app_mode)
        st.info("Deze module wordt momenteel ontwikkeld en gekoppeld aan de bestaande data-engine.")