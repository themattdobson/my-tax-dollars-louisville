#!/usr/bin/env python3
"""
aggregate.py — Download FY2025 Louisville Metro expenditure data and build budget.json

Usage:
    python3 aggregate.py

Outputs budget.json in the current directory.
Data source: Louisville Metro Open Data Portal (data.louisvilleky.gov)
Dataset ID: 260d9a7e84dc4460b75915a264d3a2f6
"""

import csv
import io
import json
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

# ── Constants ─────────────────────────────────────────────────────────────────

DATASET_ID = "260d9a7e84dc4460b75915a264d3a2f6"
FISCAL_YEAR = 2025
TOP_AGENCIES = 12   # keep this many top agencies; rest collapse into "Other"
TOP_VENDORS  = 5    # top vendors shown per agency drill-down

# Try the Louisville Metro portal first; arcgis.com is the fallback
DOWNLOAD_URLS = [
    f"https://data.louisvilleky.gov/api/download/v1/items/{DATASET_ID}/csv?layers=0",
    f"https://opendata.arcgis.com/datasets/{DATASET_ID}_0.csv",
]

# ── Department metadata ────────────────────────────────────────────────────────
# Maps lowercase name fragments → (display_name, color, description)
# The script uses the raw agency name from the data, but tries to match these
# for a friendlier display name, brand color, and subtitle.
DEPT_META = {
    "police":            ("Louisville Metro Police Dept.",   "#1a3a5c", "Patrol, investigations, training, equipment"),
    "public works":      ("Public Works & Assets",           "#3d6b2f", "Road maintenance, bridges, city facilities"),
    "health":            ("Public Health & Wellness",        "#7a2d6e", "Restaurant inspections, health programs, EMS"),
    "develop louisville":("Develop Louisville",              "#8a5a00", "Affordable housing, zoning, development"),
    "fire":              ("Louisville Fire Department",      "#b8360a", "Fire stations, apparatus, firefighter salaries"),
    "parks":             ("Parks & Recreation",              "#1a5c3a", "Metro parks, community centers, trails"),
    "kentuckiana":       ("KentuckianaWorks",                "#2d5a9e", "Job training, employment services"),
    "budget":            ("OMB Finance & Administration",    "#4a4540", "Budget mgmt, payroll, city-wide IT"),
    "omb":               ("OMB Finance & Administration",    "#4a4540", "Budget mgmt, payroll, city-wide IT"),
    "finance":           ("OMB Finance & Administration",    "#4a4540", "Budget mgmt, payroll, city-wide IT"),
    "council":           ("Metro Council & Other",           "#5c4a1a", "Council operations, community services, grants"),
    "corrections":       ("Louisville Metro Corrections",    "#5c2d1a", "Jail operations, inmate programs"),
    "animal":            ("Louisville Metro Animal Services","#2d5c4a", "Animal shelter, enforcement, adoptions"),
    "economic":          ("Economic Development",            "#6b4a2d", "Business attraction, incentives"),
    "human":             ("Human Relations Commission",      "#6b2d5c", "Civil rights, equity programs"),
    "emergency":         ("Emergency Management",            "#2d3a6b", "Disaster preparedness, 911 coordination"),
    "sustainability":    ("Office of Sustainability",        "#1a5c3a", "Climate action, energy efficiency"),
    "technology":        ("Metro Technology Services",       "#2d4a5c", "IT infrastructure, open data, cybersecurity"),
}

# Fallback colors for agencies that don't match any keyword
FALLBACK_COLORS = [
    "#4a4a4a", "#6b5a3a", "#3a5c6b", "#5c3a6b",
    "#6b3a3a", "#3a6b5c", "#5c6b3a",
]


def match_meta(agency_name: str) -> tuple[str, str, str]:
    """Return (display_name, color, desc) for the given raw agency name."""
    lower = agency_name.lower()
    for keyword, (display, color, desc) in DEPT_META.items():
        if keyword in lower:
            return display, color, desc
    return agency_name, "#4a4a4a", ""


def fmt_millions(n: float) -> str:
    return f"${n / 1_000_000:.0f}M"


# ── Download ──────────────────────────────────────────────────────────────────

raw_csv = None
used_url = None

for url in DOWNLOAD_URLS:
    try:
        print(f"Downloading from:\n  {url}")
        req = urllib.request.Request(url, headers={"User-Agent": "my-tax-dollars-louisville/1.0"})
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw_csv = resp.read().decode("utf-8-sig")  # strip BOM if present
        used_url = url
        print(f"  {len(raw_csv):,} bytes downloaded")
        break
    except Exception as e:
        print(f"  Failed: {e}")

if raw_csv is None:
    raise SystemExit("Could not download expenditure data. Check your internet connection and try again.")

# ── Parse ─────────────────────────────────────────────────────────────────────

reader = csv.DictReader(io.StringIO(raw_csv))
rows = list(reader)

if not rows:
    raise SystemExit("Downloaded CSV has no rows.")

# Normalize column names (strip whitespace, lowercase for lookup)
sample = rows[0]
col_keys = {k.strip().lower(): k for k in sample.keys()}
print(f"\n  {len(rows):,} rows · columns: {list(sample.keys())[:8]}{'...' if len(sample) > 8 else ''}")

def get(row: dict, *names: str) -> str:
    """Case-insensitive column lookup with multiple fallback names."""
    for name in names:
        canonical = col_keys.get(name.lower())
        if canonical and row.get(canonical):
            return row[canonical].strip()
    return ""


# ── Aggregate ─────────────────────────────────────────────────────────────────

agency_totals:  dict[str, float]              = defaultdict(float)
agency_vendors: dict[str, dict[str, float]]   = defaultdict(lambda: defaultdict(float))
skipped = 0

for row in rows:
    agency = get(row, "agency", "Agency", "department")
    if not agency:
        skipped += 1
        continue

    amt_str = get(row, "invoice_amount", "extended_amount", "amount")
    try:
        amt = float(amt_str.replace(",", ""))
    except (ValueError, AttributeError):
        amt = 0.0

    if amt <= 0:
        continue

    payee = get(row, "payee", "vendor", "vendor_name") or "Unknown vendor"

    agency_totals[agency] += amt
    agency_vendors[agency][payee] += amt

total_budget = sum(agency_totals.values())
print(f"\nTotal expenditures:  ${total_budget:>15,.0f}")
print(f"Agencies found:      {len(agency_totals)}")
print(f"Rows skipped:        {skipped}")

# ── Build output ──────────────────────────────────────────────────────────────

agencies_sorted = sorted(agency_totals.items(), key=lambda x: x[1], reverse=True)

# Top N agencies, the rest fold into "Other"
top = agencies_sorted[:TOP_AGENCIES]
other_total = sum(amt for _, amt in agencies_sorted[TOP_AGENCIES:])

agencies_out = []
for raw_name, total in top:
    display_name, color, desc = match_meta(raw_name)
    pct = total / total_budget if total_budget else 0

    top_vendors = sorted(agency_vendors[raw_name].items(), key=lambda x: x[1], reverse=True)[:TOP_VENDORS]
    vendors_out = [{"name": v_name, "amt": round(v_amt)} for v_name, v_amt in top_vendors]

    agencies_out.append({
        "name":    display_name,
        "raw":     raw_name,
        "total":   round(total),
        "pct":     round(pct, 4),
        "color":   color,
        "desc":    desc,
        "vendors": vendors_out,
    })

if other_total > 0 and len(agencies_sorted) > TOP_AGENCIES:
    other_pct = other_total / total_budget if total_budget else 0
    agencies_out.append({
        "name":    "Other Departments",
        "raw":     "__other__",
        "total":   round(other_total),
        "pct":     round(other_pct, 4),
        "color":   "#7a7264",
        "desc":    f"{len(agencies_sorted) - TOP_AGENCIES} smaller departments and programs",
        "vendors": [],
    })

output = {
    "fiscal_year":    FISCAL_YEAR,
    "total_budget":   round(total_budget),
    "source_url":     used_url,
    "generated_at":   datetime.now(timezone.utc).isoformat(),
    "agencies":       agencies_out,
}

with open("budget.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nWrote budget.json  ({len(agencies_out)} agencies)")
print("\nTop 5 agencies by spend:")
for a in agencies_out[:5]:
    print(f"  {a['name']:<40} {fmt_millions(a['total']):>8}  ({a['pct']*100:.1f}%)")
