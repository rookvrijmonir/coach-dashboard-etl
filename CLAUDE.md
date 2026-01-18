# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Coach Dashboard ETL is a Python ETL pipeline and analytics system for coaching/counseling referral metrics. It extracts deal and contact data from HubSpot CRM, transforms it with business logic, and visualizes it through Streamlit dashboards.

## Commands

### Setup
```bash
pip install -r requirements.txt
```

### Run ETL Pipeline
```bash
python src/main.py
```
This extracts HubSpot deals/contacts, applies business rules, and exports to `data/hubspot_export_raw.csv`. Must run before dashboards will show data.

### Run Dashboards
```bash
streamlit run src/dashboard.py              # Coach benchmark & efficiency metrics
streamlit run src/dashboard_verzekeraars.py # Insurance provider analytics
streamlit run src/capacity_dashboard.py     # Regional capacity analysis
```

### API Inspection Tool
```bash
python src/tools/api.py
```
Fetches HubSpot pipeline metadata, properties, and stage configurations.

## Architecture

### Data Flow
1. **Extract**: HubSpot API → Deals & Contacts (paginated in 14-day batches, 100 records/batch)
2. **Transform**: Business rules, metric calculations, contact enrichment
3. **Load**: CSV export to `data/hubspot_export_raw.csv`
4. **Visualize**: Streamlit dashboards consume the CSV

### Key Source Files
- `src/main.py` - ETL engine with HubSpot extraction and transformation logic
- `src/dashboard.py` - Main coach performance dashboard
- `src/dashboard_verzekeraars.py` - Insurance provider breakdown dashboard
- `src/capacity_dashboard.py` - Regional capacity analysis
- `src/tools/api.py` - HubSpot API introspection utilities

### Business Logic (main.py)
- **Status Bucket**: Maps deal stages to `actief`/`gewonnen`/`verloren`
- **Time in Stage**: Calculates duration in current pipeline stage
- **Days to Declarable**: Time until insurance declaration eligible
- **Coach Attribution**: Special handling for Nabeller pipeline (broncoach override)
- **Contact Batching**: Batch API calls to avoid rate limits

## Environment Variables

Required in `.env`:
```
HUBSPOT_ACCESS_TOKEN=<pat-na1-...>
GOOGLE_CLOUD_PROJECT=coach-dashboard-local
START_DATE_TIMESTAMP=1740787200000
```

## Notes

- Code uses Dutch variable names and comments
- Timestamps are processed as UTC
- CSV output uses semicolon delimiter
- Empty `tests/`, `src/hubspot/`, `src/bigquery/`, `src/utils/` directories are placeholders
