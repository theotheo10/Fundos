#!/usr/bin/env python3
"""
Fetches daily quota data from CVM.
CAGR windows use fixed anchor dates shared across all funds.
"""

import json, zipfile, io, math, datetime, urllib.request, calendar
from pathlib import Path

FUNDS = [
    {"name": "Tarpon GT FIF Cotas FIA",                                             "cnpj": "22232927000190", "cnpjFmt": "22.232.927/0001-90"},
    {"name": "Organon FIF Cotas FIA",                                               "cnpj": "17400251000166", "cnpjFmt": "17.400.251/0001-66"},
    {"name": "Artica Long Term FIA",                                                "cnpj": "18302338000163", "cnpjFmt": "18.302.338/0001-63"},
    {"name": "Genoa Capital Arpa CIC Classe FIM RL",                                "cnpj": "37495383000126", "cnpjFmt": "37.495.383/0001-26"},
    {"name": "Itaú Artax Ultra Multimercado FIF DA CIC RL",                         "cnpj": "42698666000105", "cnpjFmt": "42.698.666/0001-05"},
    {"name": "Guepardo Long Bias RV FIM",                                           "cnpj": "24623392000103", "cnpjFmt": "24.623.392/0001-03"},
    {"name": "Kapitalo Tarkus FIF Cotas FIA",                                       "cnpj": "28747685000153", "cnpjFmt": "28.747.685/0001-53"},
    {"name": "Real Investor FIC FIF Ações RL",                                      "cnpj": "10500884000105", "cnpjFmt": "10.500.884/0001-05"},
    {"name": "Gama Schroder Gaia Contour Tech Equity L&S BRL FIF CIC Mult IE RL",  "cnpj": "35744790000102", "cnpjFmt": "35.744.790/0001-02"},
    {"name": "Patria Long Biased FIF Cotas FIM",                                    "cnpj": "38954217000103", "cnpjFmt": "38.954.217/0001-03"},
    {"name": "Absolute Pace Long Biased FIC FIF Ações RL",                         "cnpj": "32073525000143", "cnpjFmt": "32.073.525/0001-43"},
    {"name": "Arbor FIC FIA",                                                       "cnpj": "21689246000192", "cnpjFmt": "21.689.246/0001-92"},
    {"name": "Charles River FIF Ações",                                             "cnpj": "14438229000117", "cnpjFmt": "14.438.229/0001-17"},
    {"name": "SPX Falcon FIF CIC Ações RL",                                         "cnpj": "17397315000117", "cnpjFmt": "17.397.315/0001-17"},
]

CSV_CACHE = {}
CVM_OLDEST_YEAR = 2010  # no funds older than this


def fetch_csv(year, month):
    key = (year, month)
    if key in CSV_CACHE:
        return CSV_CACHE[key]
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month:02d}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
        lines = content.split("\n")
        header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
        col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
        col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1
        print(f"  ✓ {year}-{month:02d} ({len(lines)} lines)")
        CSV_CACHE[key] = {"lines": lines, "col_date": col_date, "col_quota": col_quota}
        return CSV_CACHE[key]
    except Exception as e:
        print(f"  ✗ {year}-{month:02d}: {e}")
        CSV_CACHE[key] = None
        return None


def fund_rows_in_month(year, month, fund):
    data = fetch_csv(year, month)
    if not data or data["col_date"] < 0:
        return []
    cnpj, fmt = fund["cnpj"], fund["cnpjFmt"]
    out = []
    for line in data["lines"]:
        if cnpj not in line and fmt not in line:
            continue
        cols = line.split(";")
        try:
            d = cols[data["col_date"]].strip()
            q = float(cols[data["col_quota"]].replace(",", "."))
            if d:
                out.append({"date": d, "quota": q})
        except (ValueError, IndexError):
            continue
    out.sort(key=lambda r: r["date"])
    return out


def quota_on_or_before(target_date, fund):
    y, m = target_date.year, target_date.month
    ts = target_date.isoformat()
    for _ in range(3):
        rows = fund_rows_in_month(y, m, fund)
        candidates = [r for r in rows if r["date"] <= ts]
        if candidates:
            return candidates[-1]
        total = y * 12 + m - 2
        y, m = divmod(total, 12)
        m += 1
    return None


def subtract_months(date, n):
    total = date.year * 12 + (date.month - 1) - n
    y, m = divmod(total, 12)
    m += 1
    last_day = calendar.monthrange(y, m)[1]
    return datetime.date(y, m, min(date.day, last_day))


def years_apart(a, b):
    return (datetime.date.fromisoformat(b) - datetime.date.fromisoformat(a)).days / 365.25


def cagr(start, end, years):
    if not start or not end or years <= 0:
        return None
    return (math.pow(end / start, 1.0 / years) - 1) * 100


def find_inception(fund, anchor_year):
    """
    Find the oldest available quota for this fund independently.
    Step 1: probe December of each year from anchor_year-1 back to
            CVM_OLDEST_YEAR to find the oldest year this fund appears.
    Step 2: scan Jan→Dec of that oldest year (and the year before,
            only if December of that prior year also exists in CVM).
    """
    print(f"    inception search: {fund['cnpjFmt']}")
    oldest_year_found = anchor_year

    # Track which years had valid CVM files (not 404) so we don't scan months of missing years
    cvm_has_year = {}

    for y in range(anchor_year - 1, CVM_OLDEST_YEAR - 1, -1):
        rows = fund_rows_in_month(y, 12, fund)
        data = CSV_CACHE.get((y, 12))
        cvm_has_year[y] = data is not None  # True if file exists, False if 404
        if rows:
            oldest_year_found = y
            print(f"      found in {y}-12")
        elif oldest_year_found < anchor_year and not cvm_has_year[y]:
            # File doesn't exist in CVM at all — safe to stop
            break
        elif oldest_year_found < anchor_year and (y < oldest_year_found - 2):
            # Found something before, now 2+ years of silence — stop
            break

    # Scan month by month in oldest year (and one year earlier only if CVM has it)
    for scan_year in [oldest_year_found - 1, oldest_year_found]:
        if scan_year < CVM_OLDEST_YEAR:
            continue
        if scan_year < oldest_year_found and not cvm_has_year.get(scan_year, True):
            continue  # skip years we know don't exist in CVM
        for m in range(1, 13):
            rows = fund_rows_in_month(scan_year, m, fund)
            if rows:
                print(f"      inception: {rows[0]['date']}")
                return rows[0]

    return None


def find_anchor_date(cur_year, cur_month):
    """Most recent date with data, using first fund as probe."""
    probe = FUNDS[0]
    for delta in range(3):
        total = cur_year * 12 + cur_month - 1 - delta
        y, m = divmod(total, 12)
        m += 1
        rows = fund_rows_in_month(y, m, probe)
        if rows:
            anchor = datetime.date.fromisoformat(rows[-1]["date"])
            print(f"Anchor date: {anchor}")
            return anchor
    return datetime.date(cur_year, cur_month, 1)


def process_fund(fund, anchor):
    print(f"\n── {fund['name']}")

    latest = quota_on_or_before(anchor, fund)
    if not latest:
        print(f"  ✗ no data found")
        return {**fund, "error": True}

    print(f"  latest: {latest['quota']} on {latest['date']}")
    end_quota, end_date = latest["quota"], latest["date"]

    a12 = subtract_months(anchor, 12)
    a36 = subtract_months(anchor, 36)
    a60 = subtract_months(anchor, 60)

    q12 = quota_on_or_before(a12, fund)
    q36 = quota_on_or_before(a36, fund)
    q60 = quota_on_or_before(a60, fund)

    inception = find_inception(fund, anchor.year)
    inc_quota = inception["quota"] if inception else None
    inc_date  = inception["date"]  if inception else None

    def do_cagr(q):
        if not q:
            return None
        yrs = years_apart(q["date"], end_date)
        return cagr(q["quota"], end_quota, yrs)

    return {
        "name":          fund["name"],
        "cnpj":          fund["cnpjFmt"],
        "cnpjFmt":       fund["cnpjFmt"],
        "latestDate":    end_date,
        "inceptionDate": inc_date,
        "anchorDate":    anchor.isoformat(),
        "anchor12m":     a12.isoformat(),
        "anchor36m":     a36.isoformat(),
        "anchor60m":     a60.isoformat(),
        "cagr12":        do_cagr(q12),
        "cagr36":        do_cagr(q36),
        "cagr60":        do_cagr(q60),
        "cagrInception": cagr(inc_quota, end_quota, years_apart(inc_date, end_date)) if inc_date else None,
        "error": False,
    }


def main():
    today = datetime.date.today()
    print(f"Running for {today.isoformat()}")

    anchor = find_anchor_date(today.year, today.month)
    results = [process_fund(f, anchor) for f in FUNDS]

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "anchorDate":  anchor.isoformat(),
        "funds": results,
    }

    out_path = Path(__file__).parent.parent / "docs" / "data.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n✓ Wrote {out_path} ({len(results)} funds)")


if __name__ == "__main__":
    main()
