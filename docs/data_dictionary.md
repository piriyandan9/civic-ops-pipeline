# NYC 311 Service Requests — Data Dictionary

## Overview

This document describes the schema of the raw NYC 311 service request data
ingested from the NYC Open Data API. Each record represents one service
request (citizen complaint) submitted to NYC's 311 system.

- **Source:** https://data.cityofnewyork.us/resource/erm2-nwe9.json
- **Format:** JSON, returned as an array of records
- **Volume:** ~10,000 records added per day across NYC
- **Update cadence:** records updated multiple times per day on the source

## Critical fields (used in downstream analytics)

### Identifiers

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `unique_key` | string | No | Primary key. NYC's globally unique ID for each complaint. |

### Time fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `created_date` | datetime (ISO 8601) | No | When the complaint was filed. |
| `closed_date` | datetime (ISO 8601) | Yes | When NYC marked the complaint resolved. NULL for open complaints. |
| `due_date` | datetime (ISO 8601) | Yes | NYC's internal SLA deadline for resolution. |
| `resolution_action_updated_date` | datetime (ISO 8601) | Yes | Last time the resolution status was updated. |

### Categorical fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `agency` | string | No | Short code for handling agency (e.g., DOT, NYPD, HPD). |
| `agency_name` | string | No | Full name of handling agency. |
| `complaint_type` | string | No | Top-level category (e.g., "Pothole", "Noise - Residential"). |
| `descriptor` | string | Yes | Sub-category within complaint_type. |
| `status` | string | No | Open / Closed / In Progress / Pending. |
| `borough` | string | No | One of: MANHATTAN, BROOKLYN, QUEENS, BRONX, STATEN ISLAND, Unspecified. |

### Location fields

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `incident_address` | string | Yes | Street address where the issue was reported. |
| `incident_zip` | string | Yes | ZIP code of the incident. |
| `community_board` | string | Yes | NYC community board (e.g., "04 QUEENS"). |
| `latitude` | float | Yes | Latitude of incident. |
| `longitude` | float | Yes | Longitude of incident. |

## Known data quality issues

1. **Duplicate `unique_key` records (~0.008%)**
   - Cause: offset-based pagination drift when records are inserted at the
     source mid-fetch.
   - Mitigation: deduplication by `unique_key` at warehouse load time.
   - Tolerance: < 1% per batch.

2. **`Unspecified` borough (~0.1%)**
   - Cause: NYC could not determine the borough from the incident address.
   - Mitigation: kept as a valid value (not dropped) — represents real
     uncertainty in the source.

3. **NULL `closed_date`**
   - Expected for any complaint still open. SLA calculations must handle
     NULL closed_date gracefully (e.g., compute time-since-created instead).

## Out-of-scope fields

The raw API returns 44 columns. Only the fields above are used in downstream
models. The remaining columns (e.g., park-related fields, vehicle-specific
fields) are kept in the Bronze layer for completeness but ignored in Silver
and Gold transformations.