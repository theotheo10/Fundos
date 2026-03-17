#!/usr/bin/env python3
"""
Fetches daily quota data from CVM.
CAGR windows are anchored to fixed reference dates so all funds
are compared over exactly the same calendar periods.
"""

import json
import zipfile
import io
import math
import datetime
import urllib.request
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
CVM_OLDEST_YEAR = 2005


def fetch_csv(year: int, month: int):
    key = (year, month)
    if key in CSV_CACHE:
        return CSV_CACHE[key]
    url = (
        f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
        f"inf_diario_fi_{year}{month:02d}.zip"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
        lines = content.split("\n")
        header = lines[0].split(";")
        if header[0].startswith("\ufeff"):
            header[0] = header[0][1:]
        header = [h.strip() for h in header]
        col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
        col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1
        print(f"  ✓ fetched {year}-{month:02d} ({len(lines)} lines)")
        result = {"lines": lines, "col_date": col_date, "col_quota": col_quota}
        CSV_CACHE[key] = result
        return result
    except Exception as e:
        print(f"  ✗ {year}-{month:02d}: {e}")
        CSV_CACHE[key] = None
        return None


def fund_rows_in_month(year: int, month: int, fund: dict) -> list[dict]:
    """Return all (date, quota) pairs for this fund in the given month, sorted by date."""
    data = fetch_csv(year, month)
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return []
    cnpj, cnpjFmt = fund["cnpj"], fund["cnpjFmt"]
    result = []
    for line in data["lines"]:
        if cnpj not in line and cnpjFmt not in line:
            continue
        cols = line.split(";")
        try:
            date  = cols[data["col_date"]].strip()
            quota = float(cols[data["col_quota"]].replace(",", "."))
            if date:
                result.append({"date": date, "quota": quota})
        except (ValueError, IndexError):
            continue
    result.sort(key=lambda r: r["date"])
    return result


def quota_on_or_before(target_date: datetime.date, fund: dict):
    """
    Find the most recent quota for this fund on or before target_date.
    Searches the target month first, then goes backwards up to 3 months
    to handle holidays / weekends near month boundaries.
    """
    year, month = target_date.year, target_date.month
    target_str = target_date.isoformat()

    for _ in range(3):
        rows = fund_rows_in_month(year, month, fund)
        # Filter rows on or before target_date
        candidates = [r for r in rows if r["date"] <= target_str]
        if candidates:
            return candidates[-1]  # most recent
        # Go back one month
        total = year * 12 + month - 2
        year, month = divmod(total, 12)
        month += 1

    return None


def first_quota_ever(fund: dict, cur_year: int, cur_month: int):
    """
    Find the very first quota by scanning all years back to CVM_OLDEST_YEAR.
    Does NOT stop on 404 gaps — keeps scanning to handle archive holes.
    """
    years_with_fund = []

    # Jump by year from current year back to oldest
    y = cur_year
    while y >= CVM_OLDEST_YEAR:
        result = fund_rows_in_month(y, 12, fund)
        if result:
            years_with_fund.append(y)
            print(f"    found in {y}-12")
        y -= 1

    if not years_with_fund:
        # Fallback: try current year month by month
        years_with_fund = [cur_year]

    oldest_year = min(years_with_fund)

    # Also check the year before oldest in case fund started late in prev year
    for scan_year in [oldest_year - 1, oldest_year]:
        if scan_year < CVM_OLDEST_YEAR:
            continue
        for m in range(1, 13):
            rows = fund_rows_in_month(scan_year, m, fund)
            if rows:
                print(f"    inception: {rows[0]['date']}")
                return rows[0]

    return None


def years_apart(date_a: str, date_b: str) -> float:
    a = datetime.date.fromisoformat(date_a)
    b = datetime.date.fromisoformat(date_b)
    return (b - a).days / 365.25


def cagr(start: float, end: float, years: float):
    if not start or not end or years <= 0:
        return None
    return (math.pow(end / start, 1.0 / years) - 1) * 100


def add_months(d: datetime.date, n: int) -> datetime.date:
    """Subtract n months from date d, clamping to last day of month."""
    total = d.year * 12 + (d.month - 1) - n
    y, m = divmod(total, 12)
    m += 1
    # Clamp day to last day of resulting month
    import calendar
    last_day = calendar.monthrange(y, m)[1]
    day = min(d.day, last_day)
    return datetime.date(y, m, day)


def process_fund(fund: dict, anchor_date: datetime.date) -> dict:
    """
    anchor_date = the shared reference date (most recent trading day with data).
    All CAGR windows start from exactly anchor_date - N years,
    ensuring identical periods across all funds.
    """
    print(f"\n── {fund['name']}")

    # Latest quota — on or before anchor_date
    latest = quota_on_or_before(anchor_date, fund)
    if not latest:
        print(f"  ✗ no data found")
        return {**fund, "error": True}

    print(f"  latest: {latest['quota']} on {latest['date']} (anchor: {anchor_date})")
    end_quota = latest["quota"]
    end_date  = latest["date"]

    # Fixed anchor dates for each window — same for ALL funds
    anchor_12m = add_months(anchor_date, 12)
    anchor_36m = add_months(anchor_date, 36)
    anchor_60m = add_months(anchor_date, 60)

    print(f"  anchors → 12M:{anchor_12m} 36M:{anchor_36m} 60M:{anchor_60m}")

    q12_res = quota_on_or_before(anchor_12m, fund)
    q36_res = quota_on_or_before(anchor_36m, fund)
    q60_res = quota_on_or_before(anchor_60m, fund)

    q12 = q12_res["quota"] if q12_res else None
    q36 = q36_res["quota"] if q36_res else None
    q60 = q60_res["quota"] if q60_res else None

    # Compute exact years between anchor points for precision
    def exact_years(start_res, end_date_str):
        if not start_res:
            return None
        return years_apart(start_res["date"], end_date_str)

    y12 = exact_years(q12_res, end_date)
    y36 = exact_years(q36_res, end_date)
    y60 = exact_years(q60_res, end_date)

    inception = first_quota_ever(fund, anchor_date.year, anchor_date.month)
    inception_quota = inception["quota"] if inception else None
    inception_date  = inception["date"]  if inception else None
    inc_years = years_apart(inception_date, end_date) if inception_date else None

    result = {
        "name":          fund["name"],
        "cnpj":          fund["cnpjFmt"],
        "cnpjFmt":       fund["cnpjFmt"],
        "latestDate":    end_date,
        "inceptionDate": inception_date,
        "anchorDate":    anchor_date.isoformat(),
        "anchor12m":     anchor_12m.isoformat(),
        "anchor36m":     anchor_36m.isoformat(),
        "anchor60m":     anchor_60m.isoformat(),
        "cagr12":        cagr(q12, end_quota, y12) if q12 and y12 else None,
        "cagr36":        cagr(q36, end_quota, y36) if q36 and y36 else None,
        "cagr60":        cagr(q60, end_quota, y60) if q60 and y60 else None,
        "cagrInception": cagr(inception_quota, end_quota, inc_years) if inc_years else None,
        "error":         False,
    }
    print(f"  CAGR 12M={result['cagr12']}, 36M={result['cagr36']}, 60M={result['cagr60']}, inc={result['cagrInception']}")
    return result


def find_anchor_date(cur_year: int, cur_month: int) -> datetime.date:
    """
    Find the most recent date that has data for at least one fund.
    We use the first fund as a probe — it just needs to exist.
    """
    probe = FUNDS[0]
    for delta in range(3):
        y, m = cur_year, cur_month
        total = y * 12 + m - 1 - delta
        y2, m2 = divmod(total, 12)
        m2 += 1
        rows = fund_rows_in_month(y2, m2, probe)
        if rows:
            last = datetime.date.fromisoformat(rows[-1]["date"])
            print(f"Anchor date: {last}")
            return last
    return datetime.date(cur_year, cur_month, 1)


def main():
    today     = datetime.date.today()
    cur_year  = today.year
    cur_month = today.month

    print(f"Running for {today.isoformat()}")

    anchor = find_anchor_date(cur_year, cur_month)
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
