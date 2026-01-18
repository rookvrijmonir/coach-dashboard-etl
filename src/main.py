# src/main.py
# -----------------------------------------------------------------------------
# UNIFIED ETL ENGINE: Deals & Contacts (Batched)
# 
# AANPASSINGEN 2026:
# - Toegevoegd: patient_id (James koppeling)
# - Toegevoegd: type_begeleiding (Tariefbepaling)
# - Toegevoegd: geboortedatum_bekend (Container voorwaarde)
# -----------------------------------------------------------------------------

import os
import sys
import time
import math
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId

# --- CONFIGURATIE ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
dotenv_path = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path)

DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
os.makedirs(DATA_DIR, exist_ok=True)
EXPORT_PATH = os.path.join(DATA_DIR, 'hubspot_export_raw.csv')

BASE_URL = "https://api.hubapi.com"

# Nabeller terminal overrides die als "verloren" tellen (ongeacht probability)
NABELLER_TERMINAL_LOSS_STAGE_IDS = {"81675521", "81675523", "96512011"}

# Nabeller instroom stages (voor days_to_declarable baseline als deal in Nabeller zit)
NABELLER_INFLOW_STAGE_IDS = {"116831596", "81686449"}

# Contacteigenschappen (van het deelnemer-record)
# GEBOORTEDATUM_BEKEND toegevoegd als container voorwaarde
CONTACT_PROPS = ["aangebracht_door", "zip", "geslacht"]


def get_token() -> str:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Geen token gevonden in .env")
        sys.exit(1)
    return token


def get_client() -> HubSpot:
    return HubSpot(access_token=get_token())


def hs_get_json(path: str, params: dict | None = None) -> dict:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"GET {path} failed {r.status_code}: {r.text[:500]}")
    return r.json()


def fetch_deal_pipelines() -> list[dict]:
    data = hs_get_json("/crm/v3/pipelines/deals")
    return data.get("results", [])


def build_stage_maps(pipelines: list[dict]):
    stage_to_pipeline: dict[str, tuple[str, str]] = {}
    stage_label: dict[str, str] = {}
    stage_probability: dict[str, float | None] = {}
    nabeller_pipeline_id: str | None = None

    for p in pipelines:
        pid = str(p.get("id", ""))
        plabel = (p.get("label") or "").strip()
        if "nabeller" in plabel.lower():
            nabeller_pipeline_id = pid

        for st in p.get("stages", []) or []:
            sid = str(st.get("id", ""))
            stage_to_pipeline[sid] = (pid, plabel)
            stage_label[sid] = (st.get("label") or "").strip()
            prob = (st.get("metadata") or {}).get("probability")
            try:
                stage_probability[sid] = float(prob) if prob is not None else None
            except Exception:
                stage_probability[sid] = None

    return stage_to_pipeline, stage_label, stage_probability, nabeller_pipeline_id


# --- parsing helpers ---

def parse_to_utc_datetime(val) -> datetime | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    try:
        if s.isdigit():
            return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    except Exception:
        pass
    try:
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(s, utc=True, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime().astimezone(timezone.utc)
    except Exception:
        return None


def to_date_str(val) -> str:
    dt = parse_to_utc_datetime(val)
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d")


def format_duration_from_seconds(seconds: float | None) -> str:
    if seconds is None:
        return ""
    try:
        seconds = float(seconds)
        if math.isnan(seconds) or seconds < 0:
            return ""
    except Exception:
        return ""
    hours = seconds / 3600.0
    if hours < 24.0:
        v = round(hours, 1)
        s = f"{v:.1f}".replace(".", ",")
        return f"{s} uur"
    days = seconds / 86400.0
    v = round(days, 1)
    s = f"{v:.1f}".replace(".", ",")
    return f"{s} dagen"


def infer_seconds_from_latest_time(val) -> float | None:
    if val in (None, ""):
        return None
    try:
        x = float(val)
        if math.isnan(x) or x < 0:
            return None
        if x > 1e9:
            return x / 1000.0
        return x
    except Exception:
        return None


# --- business rules ---

def compute_status_bucket(
    dealstage: str,
    dealstage_label: str,
    is_nabeller: bool,
    mag_decl: str | None,
    stage_probability: dict[str, float | None],
) -> str:
    if mag_decl and str(mag_decl).strip():
        return "gewonnen"
    if (dealstage_label or "").strip().lower() == "afgesloten":
        return "verloren"
    if is_nabeller and dealstage in NABELLER_TERMINAL_LOSS_STAGE_IDS:
        return "verloren"
    prob = stage_probability.get(dealstage)
    if prob is not None and prob == 0.0:
        return "verloren"
    return "actief"


def compute_coach_attribuut(is_nabeller: bool, broncoach: str | None, coach_naam: str) -> str:
    if is_nabeller and broncoach and str(broncoach).strip():
        return str(broncoach).strip()
    return coach_naam


def compute_time_in_stage(
    entered_val,
    exited_val,
    latest_time_val,
    now_utc: datetime,
) -> tuple[str, str, str]:
    entered_dt = parse_to_utc_datetime(entered_val)
    exited_dt = parse_to_utc_datetime(exited_val)
    date_entered = entered_dt.strftime("%Y-%m-%d") if entered_dt else ""
    date_exited = exited_dt.strftime("%Y-%m-%d") if exited_dt else ""
    if entered_dt:
        end_dt = exited_dt or now_utc
        seconds = (end_dt - entered_dt).total_seconds()
        return format_duration_from_seconds(seconds), date_entered, date_exited
    seconds = infer_seconds_from_latest_time(latest_time_val)
    return format_duration_from_seconds(seconds), date_entered, date_exited


def run_pipeline():
    client = get_client()

    print("\n" + "=" * 60)
    print("UNIFIED ETL ENGINE: Deals & Contacts + James Container Veldnamen")
    print("=" * 60)

    pipelines = fetch_deal_pipelines()
    stage_to_pipeline, stage_label, stage_probability, nabeller_pipeline_id = build_stage_maps(pipelines)

    try:
        owner_map = {str(o.id): f"{o.first_name} {o.last_name}".strip() for o in client.crm.owners.get_all()}
    except Exception:
        owner_map = {}

    stage_ids = sorted(stage_to_pipeline.keys(), key=lambda x: int(x) if x.isdigit() else x)
    hs_v2_props = []
    for sid in stage_ids:
        hs_v2_props.extend([
            f"hs_v2_date_entered_{sid}",
            f"hs_v2_date_exited_{sid}",
            f"hs_v2_latest_time_in_{sid}",
        ])

    # DEAL properties uitgebreid met patient_id en type_begeleiding
    DEAL_PROPS = [
        "dealname", "dealstage", "hubspot_owner_id", "createdate", "closedate", "pipeline",
        "verzekeraar", "hoeveelheid_begeleiding", "record_id_contactpersoon",
        "vgz_voldoende_begeleiding", "dsw_1e_sessie_is_geweest", "datum_ig",
        "broncoach_tekst", "mag_gedeclareerd_worden_datum", "geboortedatum_bekend",
        "patient_id", "type_begeleiding", "geboortedatum_bekend", # NIEUW VOOR JAMES/TARIEVEN 2026
        *hs_v2_props,
    ]

    current_start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end_goal = datetime.now(tz=timezone.utc)
    all_deals: list[dict] = []

    while current_start < end_goal:
        current_end = current_start + timedelta(days=14)
        if current_end > end_goal:
            current_end = end_goal

        print(f"📅 Periode: {current_start.date()} tot {current_end.date()}...")
        after = 0
        while True:
            search_request = PublicObjectSearchRequest(
                filter_groups=[{"filters": [
                    {"propertyName": "createdate", "operator": "GTE", "value": int(current_start.timestamp() * 1000)},
                    {"propertyName": "createdate", "operator": "LT", "value": int(current_end.timestamp() * 1000)},
                ]}],
                properties=DEAL_PROPS,
                limit=100,
                after=after,
            )
            try:
                resp = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
                if not resp.results: break
                for d in resp.results:
                    all_deals.append({
                        **d.properties,
                        "deal_id": str(d.id)
                    })

                after = resp.paging.next.after if resp.paging and resp.paging.next else None
                if not after: break
            except ApiException as e:
                if e.status == 429:
                    time.sleep(5)
                else:
                    break
        current_start = current_end

    if not all_deals:
        print("Geen deals gevonden.")
        return

    contact_ids = list({d.get("record_id_contactpersoon") for d in all_deals if d.get("record_id_contactpersoon")})
    contact_data: dict[str, dict] = {}
    print(f"🧬 Koppelen van {len(contact_ids)} contactpersonen...")

    for i in range(0, len(contact_ids), 100):
        batch_ids = contact_ids[i:i + 100]
        batch_input = BatchReadInputSimplePublicObjectId(
            inputs=[{"id": cid} for cid in batch_ids],
            properties=CONTACT_PROPS,
        )
        try:
            c_resp = client.crm.contacts.batch_api.read(batch_read_input_simple_public_object_id=batch_input)
            for c in c_resp.results:
                contact_data[str(c.id)] = c.properties
        except Exception as e:
            print(f"⚠️ Contact batch fout: {e}")

    final_rows = []
    now_utc = datetime.now(tz=timezone.utc)

    for d in all_deals:
        dealstage = str(d.get("dealstage") or "")
        pipeline_id = str(d.get("pipeline") or "")
        dealstage_label = stage_label.get(dealstage, "")

        pipeline_label = ""
        if not pipeline_id and dealstage in stage_to_pipeline:
            pipeline_id, pipeline_label = stage_to_pipeline[dealstage]
        elif dealstage in stage_to_pipeline:
            _, pipeline_label = stage_to_pipeline[dealstage]

        is_nabeller = bool(nabeller_pipeline_id and pipeline_id == str(nabeller_pipeline_id))
        mag_decl_raw = d.get("mag_gedeclareerd_worden_datum")
        datum_declarabel = to_date_str(mag_decl_raw)

        coach_naam = owner_map.get(str(d.get("hubspot_owner_id", "")), "Onbekend")
        coach_attribuut = compute_coach_attribuut(is_nabeller, d.get("broncoach_tekst"), coach_naam)

        status_bucket = compute_status_bucket(dealstage, dealstage_label, is_nabeller, mag_decl_raw, stage_probability)

        latest_time_val = d.get(f"hs_v2_latest_time_in_{dealstage}") if dealstage else ""
        entered_val = d.get(f"hs_v2_date_entered_{dealstage}") if dealstage else ""
        exited_val = d.get(f"hs_v2_date_exited_{dealstage}") if dealstage else ""

        time_in_stage, date_entered_stage, date_exited_stage = compute_time_in_stage(entered_val, exited_val, latest_time_val, now_utc)

        days_to_declarable = ""
        if datum_declarabel:
            try:
                decl_dt = datetime.fromisoformat(datum_declarabel + "T00:00:00+00:00")
                baseline_dt = None
                if is_nabeller:
                    inflow_dts = []
                    for sid in NABELLER_INFLOW_STAGE_IDS:
                        dtv = parse_to_utc_datetime(d.get(f"hs_v2_date_entered_{sid}"))
                        if dtv: inflow_dts.append(dtv)
                    if inflow_dts: baseline_dt = min(inflow_dts)
                if not baseline_dt:
                    baseline_dt = parse_to_utc_datetime(d.get("createdate"))
                if baseline_dt:
                    days_to_declarable = round((decl_dt - baseline_dt).total_seconds() / 86400.0, 1)
            except Exception: pass

        c_id = str(d.get("record_id_contactpersoon", ""))
        cp = contact_data.get(c_id, {})

        final_rows.append({
            "deal_id": d.get("deal_id"),
            "record_id_contactpersoon": c_id,
            "dealname": d.get("dealname"),
            "createdate": d.get("createdate"),
            "closedate": d.get("closedate"),
            "dealstage": dealstage,
            "dealstage_label": dealstage_label,
            "pipeline_id": pipeline_id,
            "pipeline_label": pipeline_label,
            "coach_naam": coach_naam,
            "coach_attribuut": coach_attribuut,
            "broncoach_tekst": d.get("broncoach_tekst"),
            "status_bucket": status_bucket,
            "mag_gedeclareerd_worden_datum": mag_decl_raw,
            "datum_declarabel": datum_declarabel,
            "days_to_declarable": days_to_declarable,
            "time_in_stage": time_in_stage,
            "date_entered_stage": date_entered_stage,
            "date_exited_stage": date_exited_stage,
            "verzekeraar": d.get("verzekeraar"),
            "begeleiding": d.get("hoeveelheid_begeleiding"),
            "vgz_voldoende": d.get("vgz_voldoende_begeleiding"),
            "dsw_sessie": d.get("dsw_1e_sessie_is_geweest"),
            "datum_ig": d.get("datum_ig"),
            "patient_id": d.get("patient_id"), # NIEUW
            "type_begeleiding": d.get("type_begeleiding"), # NIEUW
            "postcode": cp.get("zip"),
            "aangebracht_door": cp.get("aangebracht_door"),
            "geslacht": cp.get("geslacht"),
            "geboortedatum_bekend": str(d.get("geboortedatum_bekend") or ""),
        })

    df = pd.DataFrame(final_rows)
    df = df[df["deal_id"].notna()]
    df = df.drop_duplicates(subset=["deal_id"])

    df.to_csv(EXPORT_PATH, index=False, sep=';', encoding='utf-8')
    print(f"✅ Succes! {len(df)} dossiers opgeslagen in {EXPORT_PATH}")


if __name__ == "__main__":
    run_pipeline()