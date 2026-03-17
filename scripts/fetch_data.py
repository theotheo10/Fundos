#!/usr/bin/env python3
"""
Fetches daily quota data from CVM for each fund.
Reads CSV as plain text (same approach as the working Google Apps Script).
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
]

CSV_CACHE = {}  # (year, month) -> {"lines": [...], "col_date": int, "col_quota": int}


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
            name = zf.namelist()[0]
            content = zf.read(name).decode("windows-1252", errors="replace")

        lines = content.split("\n")
        header = lines[0].split(";")

        # Strip BOM if present
        if header[0].startswith("\ufeff"):
            header[0] = header[0][1:]
        # Strip carriage returns
        header = [h.strip() for h in header]

        col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
        col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1

        print(f"  ✓ fetched {year}-{month:02d} ({len(lines)} lines) col_date={col_date} col_quota={col_quota}")
        print(f"    header: {header[:8]}")

        result = {"lines": lines, "col_date": col_date, "col_quota": col_quota}
        CSV_CACHE[key] = result
        return result
    except Exception as e:
        print(f"  ✗ {year}-{month:02d}: {e}")
        CSV_CACHE[key] = None
        return None


def get_rows(year: int, month: int, fund: dict):
    """Filter CSV lines matching this fund — same logic as the Apps Script indexOf."""
    data = fetch_csv(year, month)
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return data or {"lines": [], "col_date": -1, "col_quota": -1}

    cnpj    = fund["cnpj"]
    cnpjFmt = fund["cnpjFmt"]
    rows = [
        line for line in data["lines"]
        if cnpj in line or cnpjFmt in line
    ]
    rows.sort(key=lambda r: r.split(";")[data["col_date"]] if len(r.split(";")) > data["col_date"] else "")
    return {"rows": rows, "col_date": data["col_date"], "col_quota": data["col_quota"]}


def get_last_quota(year: int, month: int, fund: dict):
    result = get_rows(year, month, fund)
    rows = result.get("rows", [])
    if not rows:
        return None
    cols = rows[-1].split(";")
    try:
        return {
            "quota": float(cols[result["col_quota"]].replace(",", ".")),
            "date":  cols[result["col_date"]].strip(),
        }
    except (ValueError, IndexError):
        return None


def get_first_quota(year: int, month: int, fund: dict):
    result = get_rows(year, month, fund)
    rows = result.get("rows", [])
    if not rows:
        return None
    cols = rows[0].split(";")
    try:
        return {
            "quota": float(cols[result["col_quota"]].replace(",", ".")),
            "date":  cols[result["col_date"]].strip(),
        }
    except (ValueError, IndexError):
        return None


def subtract_months(year: int, month: int, n: int):
    total = (year * 12 + month - 1) - n
    y, m = divmod(total, 12)
    return y, m + 1


def years_apart(date_a: str, date_b: str) -> float:
    a = datetime.date.fromisoformat(date_a)
    b = datetime.date.fromisoformat(date_b)
    return (b - a).days / 365.25


def cagr(start: float, end: float, years: float):
    if not start or not end or years <= 0:
        return None
    return (math.pow(end / start, 1.0 / years) - 1) * 100


def find_inception(fund: dict, cur_year: int, cur_month: int):
    """Walk back year by year to find oldest data, then scan month by month."""
    oldest_year = cur_year

    for i in range(1, 26):
        y, _ = subtract_months(cur_year, cur_month, i * 12)
        result = get_rows(y, 12, fund)
        if result.get("rows"):
            oldest_year = y
            print(f"    found in {y}-12")
        else:
            break

    for m in range(1, 13):
        res = get_first_quota(oldest_year, m, fund)
        if res:
            print(f"    inception: {res['date']}")
            return res

    return None


def process_fund(fund: dict, cur_year: int, cur_month: int) -> dict:
    print(f"\n── {fund['name']}")

    latest = get_last_quota(cur_year, cur_month, fund)
    if not latest:
        py, pm = subtract_months(cur_year, cur_month, 1)
        latest = get_last_quota(py, pm, fund)

    if not latest:
        print(f"  ✗ no data found")
        return {**fund, "error": True}

    print(f"  latest: {latest['quota']} on {latest['date']}")
    end_quota = latest["quota"]
    end_date  = latest["date"]

    def q_at(n):
        y, m = subtract_months(cur_year, cur_month, n)
        res = get_last_quota(y, m, fund)
        return res["quota"] if res else None

    q12 = q_at(12)
    q36 = q_at(36)
    q60 = q_at(60)

    inception = find_inception(fund, cur_year, cur_month)
    inception_quota = inception["quota"] if inception else None
    inception_date  = inception["date"]  if inception else None
    inc_years = years_apart(inception_date, end_date) if inception_date else None

    result = {
        "name":          fund["name"],
        "cnpj":          fund["cnpjFmt"],
        "cnpjFmt":       fund["cnpjFmt"],
        "latestDate":    end_date,
        "inceptionDate": inception_date,
        "cagr12":        cagr(q12,             end_quota, 1)         if q12       else None,
        "cagr36":        cagr(q36,             end_quota, 3)         if q36       else None,
        "cagr60":        cagr(q60,             end_quota, 5)         if q60       else None,
        "cagrInception": cagr(inception_quota, end_quota, inc_years) if inc_years else None,
        "error":         False,
    }
    print(f"  CAGR 12M={result['cagr12']}, 36M={result['cagr36']}, 60M={result['cagr60']}, inc={result['cagrInception']}")
    return result


def main():
    today     = datetime.date.today()
    cur_year  = today.year
    cur_month = today.month

    print(f"Running for {today.isoformat()}")
    results = [process_fund(f, cur_year, cur_month) for f in FUNDS]

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "funds": results,
    }

    out_path = Path(__file__).parent.parent / "docs" / "data.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n✓ Wrote {out_path} ({len(results)} funds)")


if __name__ == "__main__":
    main()
