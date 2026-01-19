import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pathlib import Path

st.set_page_config(page_title="Doorlooptijd Dashboard", layout="wide")

# Bepaal pad relatief aan dit script: src/app.py -> ../data/
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_PATH = SCRIPT_DIR.parent / "data" / "hubspot_met_factuurdatum.csv"

@st.cache_data
def load_data(uploaded_file=None):
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file, sep=";")
    else:
        df = pd.read_csv(DATA_PATH, sep=";")
    return df

def normalize_columns(df):
    df = df.copy()
    if "mag_worden_gedeclareerd_datum" in df.columns and "mag_gedeclareerd_worden_datum" not in df.columns:
        df.rename(columns={"mag_worden_gedeclareerd_datum": "mag_gedeclareerd_worden_datum"}, inplace=True)
    return df

def parse_dates(df):
    df = df.copy()
    df["intake_datum"] = pd.to_datetime(df["intake_datum"], dayfirst=True, errors="coerce", utc=False)
    df["mag_gedeclareerd_worden_datum"] = pd.to_datetime(df["mag_gedeclareerd_worden_datum"], dayfirst=True, errors="coerce", utc=False)
    df["Factuurdatum"] = pd.to_datetime(df["Factuurdatum"], dayfirst=True, errors="coerce", utc=False)
    df["hoeveelheid_begeleiding_set_op"] = pd.to_datetime(df["hoeveelheid_begeleiding_set_op"], dayfirst=True, errors="coerce", utc=False)
    return df

def build_complete_set(df):
    required_cols = ["intake_datum", "mag_gedeclareerd_worden_datum", "Factuurdatum", "hoeveelheid_begeleiding_set_op"]
    df_complete = df.dropna(subset=required_cols)
    return df_complete

def compute_lead_times(df):
    df = df.copy()
    df["doorlooptijd_v1_dagen"] = (df["mag_gedeclareerd_worden_datum"] - df["intake_datum"]).dt.days
    df["doorlooptijd_v2_dagen"] = (df["Factuurdatum"] - df["mag_gedeclareerd_worden_datum"]).dt.days
    df["doorlooptijd_v3_dagen"] = (df["hoeveelheid_begeleiding_set_op"] - df["intake_datum"]).dt.days
    return df

def compute_summary_stats(series):
    s = series[series >= 0]
    if len(s) == 0:
        return {"mean": None, "median": None, "p25": None, "p75": None, "p10": None, "p90": None, "n": 0, "n_p25_p75": 0, "n_p10_p90": 0}
    
    p25 = s.quantile(0.25)
    p75 = s.quantile(0.75)
    p10 = s.quantile(0.10)
    p90 = s.quantile(0.90)
    
    # Tel hoeveel dossiers binnen de bandbreedtes vallen
    n_p25_p75 = ((s >= p25) & (s <= p75)).sum()
    n_p10_p90 = ((s >= p10) & (s <= p90)).sum()
    
    return {
        "mean": round(s.mean(), 1),
        "median": round(s.median(), 1),
        "p25": round(p25, 1),
        "p75": round(p75, 1),
        "p10": round(p10, 1),
        "p90": round(p90, 1),
        "n": len(s),
        "n_p25_p75": int(n_p25_p75),
        "n_p10_p90": int(n_p10_p90)
    }

def insurer_shares(df):
    df = df.copy()
    df["declarabel_bool"] = df["declarabel_status"].apply(
        lambda x: str(x).strip().lower() in ["true", "1", "yes"]
    )
    declarabel_df = df[df["declarabel_bool"]]
    totaal_declarabel = len(declarabel_df)
    if totaal_declarabel == 0:
        return pd.DataFrame()
    per_verzekeraar = declarabel_df.groupby("verzekeraar").size().reset_index(name="declarabel_count")
    per_verzekeraar["aandeel"] = per_verzekeraar["declarabel_count"] / totaal_declarabel
    per_verzekeraar = per_verzekeraar.sort_values("aandeel", ascending=False).reset_index(drop=True)
    return per_verzekeraar

def render_kpi_cards(stats):
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Gemiddelde", f"{stats['mean']} dagen" if stats['mean'] else "–")
    col2.metric("Mediaan", f"{stats['median']} dagen" if stats['median'] else "–")
    col3.metric(
        "Typische bandbreedte (P25–P75)", 
        f"{stats['p25']}–{stats['p75']} dagen" if stats['p25'] else "–",
        delta=f"{stats['n_p25_p75']} dossiers (50%)" if stats.get('n_p25_p75') else None,
        delta_color="off"
    )
    col4.metric(
        "Brede bandbreedte (P10–P90)", 
        f"{stats['p10']}–{stats['p90']} dagen" if stats['p10'] else "–",
        delta=f"{stats['n_p10_p90']} dossiers (80%)" if stats.get('n_p10_p90') else None,
        delta_color="off"
    )
    col5.metric("Aantal dossiers (n)", stats['n'])

def render_distribution(series, title, stats):
    """Render histogram met KDE (klokvorm) overlay en mediaan lijn"""
    s = series[series >= 0].dropna()
    
    fig = go.Figure()
    
    # Histogram
    fig.add_trace(go.Histogram(
        x=s,
        nbinsx=30,
        name="Aantal",
        opacity=0.7,
        histnorm="probability density"
    ))
    
    # KDE (klokvorm) berekenen
    if len(s) > 1:
        from scipy import stats as scipy_stats
        kde = scipy_stats.gaussian_kde(s)
        x_range = np.linspace(s.min(), s.max(), 200)
        kde_values = kde(x_range)
        
        fig.add_trace(go.Scatter(
            x=x_range,
            y=kde_values,
            mode="lines",
            name="Spreiding (KDE)",
            line=dict(color="red", width=2)
        ))
    
    # Mediaan lijn
    fig.add_vline(
        x=stats["median"],
        line_dash="dash",
        line_color="green",
        annotation_text=f"Mediaan: {stats['median']}",
        annotation_position="top"
    )
    
    fig.update_layout(
        title=title,
        xaxis_title="Dagen",
        yaxis_title="Dichtheid",
        showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99)
    )
    
    return fig

def render_boxplot(series, title):
    s = series[series >= 0]
    fig = px.box(s, title=title)
    fig.update_layout(yaxis_title="Dagen")
    return fig

def render_top10_table(df, lead_time_col):
    """Render top 10 langste dossiers als tabel"""
    df_pos = df[df[lead_time_col] >= 0].copy()
    df_top = df_pos.nlargest(10, lead_time_col).copy()
    
    # Selecteer relevante kolommen
    display_cols = []
    if "verzekeraar" in df_top.columns:
        display_cols.append("verzekeraar")
    display_cols.append(lead_time_col)
    
    # Hernoem kolom voor weergave
    df_display = df_top[display_cols].copy()
    df_display = df_display.rename(columns={lead_time_col: "Doorlooptijd (dagen)"})
    df_display = df_display.reset_index(drop=True)
    df_display.index = df_display.index + 1  # Start bij 1 ipv 0
    
    return df_display

def render():
    st.title("📊 Doorlooptijd Dashboard")
    
    uploaded_file = st.file_uploader("Upload CSV (hubspot_met_factuurdatum.csv)", type=["csv"])
    
    try:
        df_raw = load_data(uploaded_file)
    except Exception as e:
        st.error(f"Kon data niet laden: {e}")
        return
    
    df = normalize_columns(df_raw)
    df = parse_dates(df)
    
    n_before = len(df)
    df_complete = build_complete_set(df)
    n_after = len(df_complete)
    n_dropped = n_before - n_after
    
    st.info(f"**Dataset:** {n_after} complete dossiers (van {n_before} totaal, {n_dropped} verwijderd wegens ontbrekende waarden)")
    
    df_complete = compute_lead_times(df_complete)
    
    neg_v1 = (df_complete["doorlooptijd_v1_dagen"] < 0).sum()
    neg_v2 = (df_complete["doorlooptijd_v2_dagen"] < 0).sum()
    neg_v3 = (df_complete["doorlooptijd_v3_dagen"] < 0).sum()
    
    if neg_v1 > 0 or neg_v2 > 0 or neg_v3 > 0:
        st.warning(f"⚠️ **Negatieve doorlooptijden gedetecteerd** (uitgesloten van statistieken en grafieken):\n"
                   f"- Vraag 1: {neg_v1} dossiers\n"
                   f"- Vraag 2: {neg_v2} dossiers\n"
                   f"- Vraag 3: {neg_v3} dossiers")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Vraag 1", "Vraag 2", "Vraag 3", "Verzekeraars"])
    
    with tab1:
        st.header("Vraag 1: Intake → Toewijzing verzekeraar")
        st.markdown("""
        **Berekening:** `mag_gedeclareerd_worden_datum` − `intake_datum`
        
        *Hoeveel dagen zitten er tussen de intake en het moment dat de verzekeraar het dossier als declarabel markeert?*
        
        💡 **Typische bandbreedte (P25–P75):** Dit is het bereik waarbinnen 50% van de dossiers valt. 
        De onderste 25% en bovenste 25% worden hierbij uitgesloten, zodat je een beeld krijgt van wat "normaal" is.
        """)
        
        stats_v1 = compute_summary_stats(df_complete["doorlooptijd_v1_dagen"])
        render_kpi_cards(stats_v1)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_dist = render_distribution(df_complete["doorlooptijd_v1_dagen"], "Verdeling doorlooptijd", stats_v1)
            st.plotly_chart(fig_dist, use_container_width=True)
        with col2:
            fig_box = render_boxplot(df_complete["doorlooptijd_v1_dagen"], "Boxplot doorlooptijd")
            st.plotly_chart(fig_box, use_container_width=True)
        
        st.subheader("Top 10 langste dossiers")
        top10_v1 = render_top10_table(df_complete, "doorlooptijd_v1_dagen")
        st.dataframe(top10_v1, use_container_width=True)
    
    with tab2:
        st.header("Vraag 2: Toewijzing verzekeraar → Factuurdatum")
        st.markdown("""
        **Berekening:** `Factuurdatum` − `mag_gedeclareerd_worden_datum`
        
        *Hoeveel dagen zitten er tussen het moment dat het dossier declarabel is en de daadwerkelijke facturatie?*
        
        💡 **Typische bandbreedte (P25–P75):** Dit is het bereik waarbinnen 50% van de dossiers valt.
        Uitschieters naar boven en beneden zijn uitgefilterd voor een realistisch beeld.
        """)
        
        stats_v2 = compute_summary_stats(df_complete["doorlooptijd_v2_dagen"])
        render_kpi_cards(stats_v2)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_dist = render_distribution(df_complete["doorlooptijd_v2_dagen"], "Verdeling doorlooptijd", stats_v2)
            st.plotly_chart(fig_dist, use_container_width=True)
        with col2:
            fig_box = render_boxplot(df_complete["doorlooptijd_v2_dagen"], "Boxplot doorlooptijd")
            st.plotly_chart(fig_box, use_container_width=True)
        
        st.subheader("Top 10 langste dossiers")
        top10_v2 = render_top10_table(df_complete, "doorlooptijd_v2_dagen")
        st.dataframe(top10_v2, use_container_width=True)
    
    with tab3:
        st.header("Vraag 3: Intake → Einde traject")
        st.markdown("""
        **Berekening:** `hoeveelheid_begeleiding_set_op` − `intake_datum`
        
        *Hoeveel dagen duurt het vanaf intake tot het moment tot einde traject?*
        
        💡 **Typische bandbreedte (P25–P75):** De middelste 50% van alle trajecten valt binnen dit bereik.
        Dit geeft een goed beeld van de "normale" trajectduur.
        """)
        
        stats_v3 = compute_summary_stats(df_complete["doorlooptijd_v3_dagen"])
        render_kpi_cards(stats_v3)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_dist = render_distribution(df_complete["doorlooptijd_v3_dagen"], "Verdeling doorlooptijd", stats_v3)
            st.plotly_chart(fig_dist, use_container_width=True)
        with col2:
            fig_box = render_boxplot(df_complete["doorlooptijd_v3_dagen"], "Boxplot doorlooptijd")
            st.plotly_chart(fig_box, use_container_width=True)
        
        st.subheader("Top 10 langste dossiers")
        top10_v3 = render_top10_table(df_complete, "doorlooptijd_v3_dagen")
        st.dataframe(top10_v3, use_container_width=True)
    
    with tab4:
        st.header("Verzekeraars: aandeel declarabele deals")
        st.markdown("""
        Overzicht van welke verzekeraars het grootste aandeel hebben in de declarabele dossiers.
        Het aandeel is berekend als: `aantal declarabel per verzekeraar / totaal declarabel`.
        """)
        
        verzekeraar_df = insurer_shares(df_complete)
        
        if verzekeraar_df.empty:
            st.warning("Geen declarabele dossiers gevonden.")
        else:
            # Top 10 staafdiagram met aantallen en percentages
            top10 = verzekeraar_df.head(10).copy()
            top10["aandeel_pct"] = (top10["aandeel"] * 100).round(1)
            top10["label"] = top10.apply(lambda r: f"{r['declarabel_count']} ({r['aandeel_pct']}%)", axis=1)
            
            fig_bar = px.bar(
                top10,
                x="verzekeraar",
                y="declarabel_count",
                text="label",
                title="Top 10 verzekeraars op aandeel declarabele deals",
                labels={"verzekeraar": "Verzekeraar", "declarabel_count": "Aantal"}
            )
            fig_bar.update_traces(textposition="outside")
            fig_bar.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")
            st.plotly_chart(fig_bar, use_container_width=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Top 5 verzekeraars")
                top5 = verzekeraar_df.head(5).copy()
                fig_pie_top = px.pie(
                    top5,
                    values="declarabel_count",
                    names="verzekeraar",
                    title="Verdeling top 5 verzekeraars",
                    hole=0.4
                )
                fig_pie_top.update_traces(textposition="inside", textinfo="label+percent+value")
                st.plotly_chart(fig_pie_top, use_container_width=True)
            
            with col2:
                st.subheader("Bottom 5 verzekeraars (min. 30 declarabel)")
                verzekeraar_min30 = verzekeraar_df[verzekeraar_df["declarabel_count"] >= 30]
                bottom5 = verzekeraar_min30.tail(5).copy()
                fig_pie_bottom = px.pie(
                    bottom5,
                    values="declarabel_count",
                    names="verzekeraar",
                    title="Verdeling bottom 5 verzekeraars",
                    hole=0.4
                )
                fig_pie_bottom.update_traces(textposition="inside", textinfo="label+percent+value")
                st.plotly_chart(fig_pie_bottom, use_container_width=True)
            
            # Doorlooptijden per verzekeraar
            st.markdown("---")
            st.header("Doorlooptijden per verzekeraar")
            st.markdown("""
            Gemiddelde doorlooptijd (intake → einde traject) per verzekeraar.
            Alleen verzekeraars met minimaal 30 dossiers worden getoond.
            """)
            
            # Bereken gemiddelde doorlooptijd per verzekeraar
            df_pos = df_complete[df_complete["doorlooptijd_v3_dagen"] >= 0].copy()
            doorlooptijd_per_verz = df_pos.groupby("verzekeraar").agg(
                gemiddelde_dagen=("doorlooptijd_v3_dagen", "mean"),
                aantal_dossiers=("doorlooptijd_v3_dagen", "count")
            ).reset_index()
            
            # Filter op minimaal 30 dossiers
            doorlooptijd_min30 = doorlooptijd_per_verz[doorlooptijd_per_verz["aantal_dossiers"] >= 30].copy()
            doorlooptijd_min30["gemiddelde_dagen"] = doorlooptijd_min30["gemiddelde_dagen"].round(1)
            doorlooptijd_min30 = doorlooptijd_min30.sort_values("gemiddelde_dagen")
            
            # Totaal gemiddelde
            totaal_gem = df_pos["doorlooptijd_v3_dagen"].mean()
            
            st.metric("Totaal gemiddelde (alle verzekeraars)", f"{totaal_gem:.1f} dagen")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🚀 Top 5 snelste verzekeraars")
                snelste5 = doorlooptijd_min30.head(5).copy()
                fig_snelste = px.bar(
                    snelste5,
                    x="verzekeraar",
                    y="gemiddelde_dagen",
                    text="gemiddelde_dagen",
                    title="Snelste verzekeraars (gem. dagen)",
                    color="gemiddelde_dagen",
                    color_continuous_scale="Greens_r"
                )
                fig_snelste.update_traces(texttemplate="%{text} dagen", textposition="outside")
                fig_snelste.update_layout(showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig_snelste, use_container_width=True)
            
            with col2:
                st.subheader("🐢 Top 5 traagste verzekeraars")
                traagste5 = doorlooptijd_min30.tail(5).sort_values("gemiddelde_dagen", ascending=False).copy()
                fig_traagste = px.bar(
                    traagste5,
                    x="verzekeraar",
                    y="gemiddelde_dagen",
                    text="gemiddelde_dagen",
                    title="Traagste verzekeraars (gem. dagen)",
                    color="gemiddelde_dagen",
                    color_continuous_scale="Reds"
                )
                fig_traagste.update_traces(texttemplate="%{text} dagen", textposition="outside")
                fig_traagste.update_layout(showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig_traagste, use_container_width=True)

if __name__ == "__main__":
    render()