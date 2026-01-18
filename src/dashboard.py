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


# --- DECIMALEN: max 1 decimaal, maar .0 weglaten ---
def fmt_1dec_drop0(x):
    if x is None:
        return "-"
    try:
        if pd.isna(x):
            return "-"
    except Exception:
        pass
    try:
        v = float(x)
    except Exception:
        return x
    s = f"{v:.1f}"
    return s[:-2] if s.endswith(".0") else s


def format_df_for_display(df_in: pd.DataFrame) -> pd.DataFrame:
    out = df_in.copy()
    num_cols = out.select_dtypes(include=["number"]).columns
    for c in num_cols:
        out[c] = out[c].map(fmt_1dec_drop0)
    return out


def load_data():
    if not os.path.exists('data/hubspot_export_raw.csv'):
        st.error("CSV niet gevonden. Draai main.py."); return None

    df = pd.read_csv('data/hubspot_export_raw.csv', sep=';')

    # --- Coach attributie (Nabeller -> broncoach) ---
    if 'coach_attribuut' in df.columns:
        df['coach_naam'] = df['coach_attribuut']
    else:
        df['coach_naam'] = df.get('coach_naam', pd.Series('Onbekend', index=df.index))
    df['coach_naam'] = df['coach_naam'].astype(str).replace('nan', 'Onbekend')

    date_cols = ['createdate', 'ts_warme_aanvraag', 'ts_in_begeleiding', 'datum_afgesloten', 'datum_ig', 'date_entered_stage', 'date_exited_stage', 'mag_gedeclareerd_worden_datum', 'datum_declarabel']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)

    df['stage_id_str'] = df['dealstage'].astype(str)

    # Actief/gewonnen/verloren: primair via status_bucket (ETL)
    if 'status_bucket' in df.columns:
        # FIX: Als het een TERMINAL stage is, mag hij NOOIT actief zijn, wat status_bucket ook zegt
        is_terminal = df['stage_id_str'].isin(TERMINAL_STAGES.keys())
        df['Is_Actief'] = (df['status_bucket'].astype(str) == 'actief') & (~is_terminal)
    else:
        df['Is_Actief'] = df['stage_id_str'].isin(ACTIVE_STAGES.keys())

    all_maps = {**ACTIVE_STAGES, **TERMINAL_STAGES}
    df['Fase'] = df['stage_id_str'].map(all_maps)

    # Afgesloten zonder declarabel (status_bucket=verloren) hoort niet onder "Afgesloten / Gewonnen"
    if 'status_bucket' in df.columns and 'dealstage_label' in df.columns:
        mask_closed_lost = (
            (df['status_bucket'].astype(str) == 'verloren')
            & (df['dealstage_label'].astype(str).str.strip().str.lower() == 'afgesloten')
        )
        df.loc[mask_closed_lost, 'Fase'] = "Geen interesse / Verloren"

    df.loc[df['Fase'].isna() & ~df['Is_Actief'], 'Fase'] = "Geen interesse / Verloren"
    df['Fase'] = df['Fase'].fillna("Overig")

    nu = pd.Timestamp.now()

    # baseline = date_entered_stage (nieuw, nauwkeurig), fallback = ts_warme_aanvraag (oud), fallback = createdate
    if 'date_entered_stage' in df.columns:
        entered = df['date_entered_stage'].copy()
    else:
        entered = pd.Series(pd.NaT, index=df.index)

    if 'ts_warme_aanvraag' in df.columns:
        entered = entered.fillna(df['ts_warme_aanvraag'])

    entered = entered.fillna(df['createdate'])

    df['dagen_in_fase'] = (nu - entered).dt.days

    # --- DECLARATIE LOGICA (conform jouw verzekeraargroepen-document) ---
    def determine_declarable_status(row):
        verz = str(row.get('verzekeraar', '')).lower().strip()
        begeleiding = str(row.get('begeleiding', '')).strip()

        dt_in_begeleiding = row.get('ts_in_begeleiding', pd.NaT)
        if pd.isnull(dt_in_begeleiding) and str(row.get('stage_id_str')) == "15413223":
            dt_in_begeleiding = row.get('date_entered_stage', pd.NaT)
        if pd.isnull(dt_in_begeleiding):
            dt_in_begeleiding = row.get('createdate', pd.NaT)

        dt_afgesloten = row.get('datum_afgesloten', pd.NaT)
        if pd.isnull(dt_afgesloten) and str(row.get('stage_id_str')) == "15413226":
            dt_afgesloten = row.get('date_entered_stage', pd.NaT)
        if pd.isnull(dt_afgesloten):
            dt_afgesloten = row.get('createdate', pd.NaT)

        dec_date = pd.NaT

        if any(v in verz for v in VGZ_GROUP):
            if str(row.get('vgz_voldoende', '')).strip() == 'Ja' and pd.notnull(dt_afgesloten):
                if dt_afgesloten > pd.Timestamp('2024-11-01'):
                    dec_date = dt_afgesloten

        elif any(d in verz for d in DSW_GROUP):
            if str(row.get('dsw_sessie', '')).strip() == 'Ja' and pd.notnull(dt_in_begeleiding):
                if dt_in_begeleiding >= pd.Timestamp('2024-08-01'):
                    dec_date = dt_in_begeleiding

        else:
            if verz and verz not in {'nan', 'none'} and pd.notnull(dt_in_begeleiding):
                if dt_in_begeleiding > pd.Timestamp('2024-11-01'):
                    dec_date = dt_in_begeleiding

        if pd.isnull(dec_date):
            return pd.NaT, "Niet declarabel"
            

        # --- DROP-OUT / VOLLEDIG BEPALING OP BASIS VAN HOEVEELHEID_BEGELEIDING ---
        DROP_OUT_VALUES = [
            "alleen",
            "minder dan de helft"
        ]

        is_dropout = any(v in begeleiding.lower() for v in DROP_OUT_VALUES)

        return dec_date, "Dropout" if is_dropout else "Volledig"



    df[['datum_declarabel', 'Declaratie_Type']] = df.apply(lambda r: pd.Series(determine_declarable_status(r)), axis=1)

    if 'days_to_declarable' in df.columns:
        df['dagen_tot_geld'] = pd.to_numeric(df['days_to_declarable'], errors='coerce')
    else:
        df['dagen_tot_geld'] = (df['datum_declarabel'] - df['createdate']).dt.days

    if 'status_bucket' in df.columns and 'Declaratie_Type' in df.columns:

        # Verloren = HubSpot-verloren (blijft ongewijzigd)
        df['Conv_Verloren'] = (df['status_bucket'].astype(str) == "verloren")

        # Gewonnen = beleidsmatig declarabel
        df['Conv_Gewonnen'] = df['Declaratie_Type'].isin([
            "Dropout",
            "Volledig"
        ])

    return df


def styled_table(df_summary):
    """Modern hoog-contrast styling voor maximale leesbaarheid."""
    # FIX: transpose eerst, en style daarna (anders raak je de styler kwijt)
    df_t = df_summary.T
    return df_t.style.format(fmt_1dec_drop0, na_rep="-")\
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
        ])


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
        labels = [f"{int(v)} ({(v/total*100 if total>0 else 0):.1f}%)" for v in counts.values]
        fig_overall = px.bar(x=counts.index, y=counts.values, text=labels, color=counts.values, color_continuous_scale='Blues')
        fig_overall.update_traces(textposition='outside')
        st.plotly_chart(fig_overall, use_container_width=True)

        st.divider()

        # --- 2. OPBRENGSTVERDELING (Drop-out vs Volledig) ---
        st.subheader("💰 Opbrengstverdeling Declarabel")

        TARIEVEN = {
            "Volledig": 205,
            "Dropout": 102.5
        }

        rev_df = df[
            (df['createdate'] >= grens_datum) &
            (df['Declaratie_Type'].isin(["Volledig", "Dropout"]))
        ].copy()

        # aantallen per coach + type
        grp = (
            rev_df
            .groupby(['coach_naam', 'Declaratie_Type'])
            .size()
            .reset_index(name='Aantal')
        )

        # bedrag berekenen
        grp['Tarief'] = grp['Declaratie_Type'].map(TARIEVEN)
        grp['Bedrag'] = grp['Aantal'] * grp['Tarief']

        # benchmark = gemiddelde per coach
        bench = (
            grp
            .groupby(['coach_naam', 'Declaratie_Type'])[['Aantal', 'Bedrag']]
            .sum()
            .reset_index()
            .groupby('Declaratie_Type')[['Aantal', 'Bedrag']]
            .mean()
            .reset_index()
        )

        # afronden: aantallen 1 dec, bedragen hele euro's
        bench['Aantal'] = bench['Aantal'].round(1)
        bench['Bedrag'] = bench['Bedrag'].round(0)


        bench['coach_naam'] = 'Gemiddelde (Benchmark)'


        plot_df = pd.concat([grp, bench], ignore_index=True)

        # percentages per coach
        plot_df['Totaal'] = plot_df.groupby('coach_naam')['Aantal'].transform('sum')
        plot_df['Percentage'] = (plot_df['Aantal'] / plot_df['Totaal'] * 100).round(1)

        plot_df = plot_df[
            plot_df['coach_naam'].isin(sel_coaches + ['Gemiddelde (Benchmark)'])
        ]

        plot_df['Label'] = (
            plot_df['Aantal'].apply(fmt_1dec_drop0)
            + " | "
            + plot_df['Percentage'].apply(fmt_1dec_drop0) + "%"
            + "\n€" + plot_df['Bedrag'].round(0).astype(int).astype(str)
        )


        fig = px.bar(
            plot_df,
            x='coach_naam',
            y='Aantal',
            color='Declaratie_Type',
            barmode='group',
            text='Label',
            color_discrete_map={
                'Volledig': '#2c3e50',
                'Dropout': '#e67e22'
            }
        )

        fig.update_traces(textposition='outside')

        # --- Y-as netjes formatteren (1 decimaal, .0 weg) ---
        all_y = []
        for t in fig.data:
            all_y.extend(t.y)

        ticks = sorted(set(all_y))

        fig.update_yaxes(
            tickvals=ticks,
            ticktext=[fmt_1dec_drop0(v) for v in ticks]
        )

        st.plotly_chart(fig, use_container_width=True)
        st.divider()


                # --- 2B. GEMIDDELDE DOORLOOPTIJD TOT DECLARATIE ---
        st.subheader("⏱️ Gemiddelde doorlooptijd tot Drop-out / Volledig")

        time_df = df[
            (df['createdate'] >= grens_datum) &
            (df['Declaratie_Type'].isin(['Dropout', 'Volledig'])) &
            (df['dagen_tot_geld'].notna())
        ].copy()

        if not time_df.empty:
            # gemiddelde per coach + type
            avg_time = (
                time_df
                .groupby(['coach_naam', 'Declaratie_Type'])['dagen_tot_geld']
                .mean()
                .reset_index()
                .round(1)
            )

            # benchmark = gemiddelde over alle coaches
            bench_time = (
                avg_time
                .groupby('Declaratie_Type')['dagen_tot_geld']
                .mean()
                .reset_index()
                .round(1)
            )
            bench_time['coach_naam'] = 'Gemiddelde (Benchmark)'

            plot_time = pd.concat([avg_time, bench_time], ignore_index=True)

            plot_time = plot_time[
                plot_time['coach_naam'].isin(sel_coaches + ['Gemiddelde (Benchmark)'])
            ]

            plot_time['Label'] = plot_time['dagen_tot_geld'].apply(fmt_1dec_drop0)

            fig_time = px.bar(
                plot_time,
                x='coach_naam',
                y='dagen_tot_geld',
                color='Declaratie_Type',
                barmode='group',
                text='Label',
                color_discrete_map={
                    'Volledig': '#2c3e50',
                    'Dropout': '#e67e22'
                }
            )

            fig_time.update_traces(textposition='outside')

            # Y-as netjes formatteren (1 decimaal, .0 weg)
            all_y = []
            for t in fig_time.data:
                all_y.extend(t.y)

            ticks = sorted(set(all_y))

            fig_time.update_yaxes(
                tickvals=ticks,
                ticktext=[fmt_1dec_drop0(v) for v in ticks],
                title="Gemiddelde dagen"
            )

            st.plotly_chart(fig_time, use_container_width=True)

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
            return [
                {'Coach': label, 'Type': 'Gewonnen', 'Aantal': round(float(w), 1), 'Percentage': round((w/tot*100), 1)},
                {'Coach': label, 'Type': 'Verloren', 'Aantal': round(float(l), 1), 'Percentage': round((l/tot*100), 1)}
            ]

        conv_plot_data = []
        bench_data = build_conv_df(df_period, "Gemiddelde (Benchmark)")
        if bench_data:
            for item in bench_data:
                item['Aantal'] = round(item['Aantal'] / num_total_coaches, 1)
            conv_plot_data.extend(bench_data)
        for cn in sel_coaches:
            cr = build_conv_df(df_period[df_period['coach_naam']==cn], cn)
            if cr: conv_plot_data.extend(cr)

        if conv_plot_data:
            df_c = pd.DataFrame(conv_plot_data)
            df_c['Label'] = df_c.apply(lambda r: f"{r['Aantal']} ({r['Percentage']}%)", axis=1)
            fig_conv = px.bar(df_c, x="Coach", y="Aantal", color="Type", text="Label", barmode="group",
                              color_discrete_map={'Gewonnen':'#27ae60','Verloren':'#c0392b'})
            fig_conv.update_traces(textposition='outside')
            st.plotly_chart(fig_conv, use_container_width=True)

        st.divider()

        # --- 5. PERFORMANCE PROFIELEN ---
        st.subheader("📋 Performance Profielen (Detailoverzicht)")
        with st.expander("🏢 ORGANISATIE GEMIDDELDE (Benchmark)", expanded=True):
            st.table(styled_table(create_summary(df_display, num_total_coaches)))

        for coach in sel_coaches:
            c_df = df_display[df_display['coach_naam']==coach]
            c_all = df_period[df_period['coach_naam']==coach]
            with st.container():
                st.markdown(f"#### 👤 Coach: **{coach}**")
                w, l = c_all['Conv_Gewonnen'].sum(), c_all['Conv_Verloren'].sum()
                ratio = round((w/(w+l)*100), 1) if (w+l)>0 else 0
                col_m, col_t = st.columns([1,4])
                col_m.metric("Succesratio (G/V)", f"{ratio}%")
                col_t.table(styled_table(create_summary(c_df)))
                with st.expander(f"Dossier details voor {coach}"):
                    details = c_df[['deal_id','Fase','dagen_in_fase','createdate']].sort_values('dagen_in_fase', ascending=False)
                    st.dataframe(format_df_for_display(details),
                                 use_container_width=True, hide_index=True)
                st.write("<br>", unsafe_allow_html=True)
    else:
        st.title(app_mode)
        st.info("Deze module wordt momenteel ontwikkeld en gekoppeld aan de bestaande data-engine.")
