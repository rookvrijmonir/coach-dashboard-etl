#!/usr/bin/env python3
import os
import sys
import csv
import json
import re
from typing import Dict, List, Any, Tuple

import requests
from dotenv import load_dotenv

BASE = "https://api.hubapi.com"

# Zelfde .env padlogica als in main.py
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

NEEDED_BUSINESS_PROPS = [
    "mag_gedeclareerd_worden_datum",
    "broncoach_tekst",
    "record_id_contactpersoon",
    "dealstage",
    "hubspot_owner_id",
]

HSV2_PATTERNS = {
    "latest_time_in": re.compile(r"^hs_v2_latest_time_in_(\d+)$"),
    "date_entered": re.compile(r"^hs_v2_date_entered_(\d+)$"),
    "date_exited": re.compile(r"^hs_v2_date_exited_(\d+)$"),
    "cumulative_time_in": re.compile(r"^hs_v2_cumulative_time_in_(\d+)$"),
}


def get_token() -> str:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token or "plak_hier" in token:
        print("FOUT: Geen token gevonden in .env", file=sys.stderr)
        sys.exit(1)
    return token


def hs_get(path: str, token: str, params: Dict[str, Any] | None = None) -> Any:
    url = f"{BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"GET {path} failed {r.status_code}: {r.text[:500]}")
    return r.json()


def fetch_deal_pipelines(token: str) -> Dict[str, Any]:
    return hs_get("/crm/v3/pipelines/deals", token)


def fetch_deal_properties(token: str) -> List[Dict[str, Any]]:
    props: List[Dict[str, Any]] = []
    after = None
    while True:
        params: Dict[str, Any] = {"limit": 100}
        if after:
            params["after"] = after
        data = hs_get("/crm/v3/properties/deals", token, params=params)
        props.extend(data.get("results", []))
        paging = data.get("paging", {})
        nxt = (paging.get("next") or {}).get("after")
        if not nxt:
            break
        after = nxt
    return props


def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def stage_index(pipelines_json: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    stage_map: Dict[str, Dict[str, str]] = {}
    stage_rows: List[Dict[str, str]] = []

    for p in pipelines_json.get("results", []):
        pid = str(p.get("id", ""))
        plabel = p.get("label", "")
        for st in p.get("stages", []):
            sid = str(st.get("id", ""))
            row = {
                "pipeline_id": pid,
                "pipeline_label": plabel,
                "stage_id": sid,
                "stage_label": st.get("label", ""),
                "display_order": str(st.get("displayOrder", "")),
                "probability": str(st.get("metadata", {}).get("probability", "")),
            }
            stage_map[sid] = row
            stage_rows.append(row)

    return stage_map, stage_rows


def build_hsv2_matrix(props: List[Dict[str, Any]], stage_map: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    matrix: Dict[str, Dict[str, bool]] = {}

    for p in props:
        name = p.get("name", "")
        for key, rx in HSV2_PATTERNS.items():
            m = rx.match(name)
            if m:
                sid = m.group(1)
                matrix.setdefault(sid, {k: False for k in HSV2_PATTERNS.keys()})
                matrix[sid][key] = True

    rows: List[Dict[str, str]] = []
    for sid, flags in sorted(matrix.items(), key=lambda x: int(x[0])):
        meta = stage_map.get(sid, {})
        rows.append({
            "stage_id": sid,
            "pipeline_label": meta.get("pipeline_label", ""),
            "stage_label": meta.get("stage_label", ""),
            "probability": meta.get("probability", ""),
            "has_latest_time_in": "1" if flags["latest_time_in"] else "0",
            "has_date_entered": "1" if flags["date_entered"] else "0",
            "has_date_exited": "1" if flags["date_exited"] else "0",
            "has_cumulative_time_in": "1" if flags["cumulative_time_in"] else "0",
        })
    return rows


def main() -> int:
    token = get_token()

    pipelines = fetch_deal_pipelines(token)
    props = fetch_deal_properties(token)

    # 1) Pipelines + stages (raw)
    write_json("pipelines_deals.json", pipelines)

    # 2) Properties subset: hs_v2_* + business props
    rows_props: List[Dict[str, Any]] = []
    for p in props:
        name = p.get("name", "")
        if name.startswith("hs_v2_") or name in NEEDED_BUSINESS_PROPS:
            rows_props.append({
                "name": name,
                "label": p.get("label", ""),
                "type": p.get("type", ""),
                "fieldType": p.get("fieldType", ""),
                "groupName": p.get("groupName", ""),
            })
    rows_props.sort(key=lambda r: r["name"])
    write_csv(
        "deal_properties_hs_v2.csv",
        rows_props,
        ["name", "label", "type", "fieldType", "groupName"],
    )

    # 3) Flatten pipelines/stages
    stage_map, stage_rows = stage_index(pipelines)
    write_csv(
        "pipelines_stages.csv",
        stage_rows,
        ["pipeline_id", "pipeline_label", "stage_id", "stage_label", "display_order", "probability"],
    )

    # 4) Matrix: per stage_id welke hs_v2 properties bestaan
    matrix_rows = build_hsv2_matrix(props, stage_map)
    write_csv(
        "stage_property_matrix.csv",
        matrix_rows,
        ["stage_id", "pipeline_label", "stage_label", "probability",
         "has_latest_time_in", "has_date_entered", "has_date_exited", "has_cumulative_time_in"],
    )

    print("OK: geschreven: pipelines_deals.json, pipelines_stages.csv, deal_properties_hs_v2.csv, stage_property_matrix.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
