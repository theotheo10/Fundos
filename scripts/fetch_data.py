#!/usr/bin/env python3
"""
Fetches daily quota data from CVM.
- Monthly files for 2021-present: /INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip
- Annual files for pre-2021:      /INF_DIARIO/DADOS/HIST/inf_diario_fi_YYYY.zip
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

# Monthly cache: (year, month) -> parsed data
# Annual cache:  year -> parsed data  (for HIST files)
MONTHLY_CACHE = {}
ANNUAL_CACHE  = {}

FIRST_MONTHLY_YEAR = 2021  # CVM monthly files start here
CVM_OLDEST_YEAR    = 2005  # HIST annual files go back to here


def _parse_content(content):
    lines = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
    col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1
    return {"lines": lines, "col_date": col_date, "col_quota": col_quota}


def fetch_monthly(year, month):
    key = (year, month)
    if key in MONTHLY_CACHE:
        return MONTHLY_CACHE[key]
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month:02d}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
        result = _parse_content(content)
        print(f"  ✓ monthly {year}-{month:02d} ({len(result['lines'])} lines)")
        MONTHLY_CACHE[key] = result
        return result
    except Exception as e:
        print(f"  ✗ monthly {year}-{month:02d}: {e}")
        MONTHLY_CACHE[key] = None
        return None


def fetch_annual(year):
    if year in ANNUAL_CACHE:
        return ANNUAL_CACHE[year]
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
        result = _parse_content(content)
        print(f"  ✓ annual  {year} ({len(result['lines'])} lines)")
        ANNUAL_CACHE[year] = result
        return result
    except Exception as e:
        print(f"  ✗ annual  {year}: {e}")
        ANNUAL_CACHE[year] = None
        return None


def _extract_rows(data, fund):
    """Extract sorted (date, quota) rows for a fund from a parsed CSV block."""
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


def rows_in_month(year, month, fund):
    return _extract_rows(fetch_monthly(year, month), fund)


def rows_in_year(year, fund):
    return _extract_rows(fetch_annual(year), fund)


def quota_on_or_before(target_date, fund):
    """
    Most recent quota on or before target_date.
    Uses monthly files for 2021+, annual files for pre-2021.
    Looks back up to 3 periods if the target period has no data.
    """
    ts = target_date.isoformat()
    y, m = target_date.year, target_date.month

    # Search up to 3 months/years back
    for _ in range(3):
        if y >= FIRST_MONTHLY_YEAR:
            rows = rows_in_month(y, m, fund)
        else:
            rows = rows_in_year(y, fund)

        candidates = [r for r in rows if r["date"] <= ts]
        if candidates:
            return candidates[-1]

        # Go back one period
        if y >= FIRST_MONTHLY_YEAR:
            total = y * 12 + m - 2
            y, m = divmod(total, 12)
            m += 1
        else:
            y -= 1

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
    Find oldest quota using:
    - Annual HIST files for years before 2021
    - Monthly files for 2021+
    Scans December (monthly) or full year (annual) as probe,
    then scans month-by-month / day-by-day within the oldest year found.
    """
    print(f"    inception search: {fund['cnpjFmt']}")

    oldest_year_found = anchor_year
    cvm_has_year = {}  # track which years exist in CVM

    # Step 1: probe each year going backwards
    for y in range(anchor_year - 1, CVM_OLDEST_YEAR - 1, -1):
        if y >= FIRST_MONTHLY_YEAR:
            rows = rows_in_month(y, 12, fund)
            data = MONTHLY_CACHE.get((y, 12))
        else:
            rows = rows_in_year(y, fund)
            data = ANNUAL_CACHE.get(y)

        cvm_has_year[y] = data is not None

        if rows:
            oldest_year_found = y
            print(f"      found in {y}")
        elif not cvm_has_year[y]:
            # File doesn't exist — keep going (might be a gap in CVM archive)
            pass
        elif oldest_year_found < anchor_year:
            # File exists but fund not in it — we've gone past inception
            break

    print(f"      oldest year: {oldest_year_found}")

    # Step 2: find exact first date within oldest_year_found
    # Also check the year before in case fund started late in prior year
    for scan_year in [oldest_year_found - 1, oldest_year_found]:
        if scan_year < CVM_OLDEST_YEAR:
            continue
        if not cvm_has_year.get(scan_year, True):
            continue  # year has no CVM file at all

        if scan_year >= FIRST_MONTHLY_YEAR:
            # Scan month by month
            for m in range(1, 13):
                rows = rows_in_month(scan_year, m, fund)
                if rows:
                    print(f"      inception: {rows[0]['date']}")
                    return rows[0]
        else:
            # Annual file — get first entry directly
            rows = rows_in_year(scan_year, fund)
            if rows:
                print(f"      inception: {rows[0]['date']}")
                return rows[0]

    return None


def find_anchor_date(cur_year, cur_month):
    probe = FUNDS[0]
    for delta in range(3):
        total = cur_year * 12 + cur_month - 1 - delta
        y, m = divmod(total, 12)
        m += 1
        rows = rows_in_month(y, m, probe)
        if rows:
            anchor = datetime.date.fromisoformat(rows[-1]["date"])
            print(f"Anchor date: {anchor}")
            return anchor
    return datetime.date(cur_year, cur_month, 1)


def process_fund(fund, anchor, prev_max_quotas):
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

    # ── Max quota: carry forward previous, update if current is higher ──
    prev = prev_max_quotas.get(fund["cnpjFmt"], {})
    prev_max = prev.get("maxQuota", 0) or 0
    if end_quota >= prev_max:
        max_quota      = end_quota
        max_quota_date = end_date
        print(f"  new max quota: {max_quota} on {max_quota_date}")
    else:
        max_quota      = prev_max
        max_quota_date = prev.get("maxQuotaDate", "")
        print(f"  max quota unchanged: {max_quota} on {max_quota_date}")

    result = {
        "name":          fund["name"],
        "cnpj":          fund["cnpjFmt"],
        "cnpjFmt":       fund["cnpjFmt"],
        "latestDate":    end_date,
        "latestQuota":   end_quota,
        "maxQuota":      max_quota,
        "maxQuotaDate":  max_quota_date,
        "inceptionDate": inc_date,
        "anchorDate":    anchor.isoformat(),
        "anchor12m":     a12.isoformat(),
        "anchor36m":     a36.isoformat(),
        "anchor60m":     a60.isoformat(),
        "cagr12":        do_cagr(q12),
        "cagr36":        do_cagr(q36),
        "cagr60":        do_cagr(q60),
        "cagrInception": cagr(inc_quota, end_quota, years_apart(inc_date, end_date)) if inc_date else None,
        "error":         False,
    }
    print(f"  CAGR 12M={result['cagr12']}, 36M={result['cagr36']}, 60M={result['cagr60']}, inc={result['cagrInception']}")
    return result


def fetch_cdi(anchor: datetime.date, a12: datetime.date, a36: datetime.date, a60: datetime.date):
    """Fetch CDI daily rates from Banco Central and compute CAGRs."""
    start = a60 - datetime.timedelta(days=10)
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
           f"?formato=json"
           f"&dataInicial={start.strftime('%d/%m/%Y')}"
           f"&dataFinal={anchor.strftime('%d/%m/%Y')}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        # price_map: date -> accumulated index (starting at 1.0)
        price_map = {}
        acc = 1.0
        for entry in data:
            d = datetime.datetime.strptime(entry["data"], "%d/%m/%Y").date().isoformat()
            acc *= (1 + float(entry["valor"]) / 100)
            price_map[d] = acc

        dates = sorted(price_map.keys())

        def best_price(target: datetime.date):
            tstr = target.isoformat()
            candidates = [d for d in dates if d <= tstr]
            return price_map[candidates[-1]] if candidates else None

        def actual_date(target):
            tstr = target.isoformat()
            candidates = [d for d in dates if d <= tstr]
            return candidates[-1] if candidates else None

        p_anchor = best_price(anchor)
        p12 = best_price(a12); p36 = best_price(a36); p60 = best_price(a60)
        d_anchor = actual_date(anchor)
        d12 = actual_date(a12); d36 = actual_date(a36); d60 = actual_date(a60)

        def cdi_cagr(start_d, end_d, p_start, p_end):
            if not p_start or not p_end: return None
            yrs = (datetime.date.fromisoformat(end_d) - datetime.date.fromisoformat(start_d)).days / 365.25
            return cagr(p_start, p_end, yrs)

        result_cdi = {
            "cagr12": cdi_cagr(d12,  d_anchor, p12,  p_anchor) if d12  else None,
            "cagr36": cdi_cagr(d36,  d_anchor, p36,  p_anchor) if d36  else None,
            "cagr60": cdi_cagr(d60,  d_anchor, p60,  p_anchor) if d60  else None,
        }
        print(f"  CDI  12M={result_cdi['cagr12']:.2f}% 36M={result_cdi['cagr36']:.2f}% 60M={result_cdi['cagr60']:.2f}%")
        return result_cdi
    except Exception as e:
        print(f"  ✗ CDI fetch failed: {e}")
        return {"cagr12": None, "cagr36": None, "cagr60": None}


    """Fetch IBOVESPA historical prices from Yahoo Finance and compute CAGRs."""
    ticker = "%5EBVSP"
    period1 = int((datetime.datetime.combine(a60 - datetime.timedelta(days=10), datetime.time()) 
                   .replace(tzinfo=datetime.timezone.utc)).timestamp())
    period2 = int((datetime.datetime.combine(anchor + datetime.timedelta(days=5), datetime.time())
                   .replace(tzinfo=datetime.timezone.utc)).timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&period1={period1}&period2={period2}"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        result  = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes     = result["indicators"]["quote"][0]["close"]

        price_map = {}
        for ts, price in zip(timestamps, closes):
            if price is not None:
                d = datetime.datetime.utcfromtimestamp(ts).date().isoformat()
                price_map[d] = price

        dates = sorted(price_map.keys())

        def best_price(target: datetime.date):
            tstr = target.isoformat()
            candidates = [d for d in dates if d <= tstr]
            return price_map[candidates[-1]] if candidates else None

        def ibov_cagr(start_date, end_date, p_start, p_end):
            if not p_start or not p_end:
                return None
            yrs = (datetime.date.fromisoformat(end_date) - datetime.date.fromisoformat(start_date)).days / 365.25
            return cagr(p_start, p_end, yrs)

        p_anchor = best_price(anchor)
        p12      = best_price(a12)
        p36      = best_price(a36)
        p60      = best_price(a60)

        # Use actual dates found for precision
        def actual_date(target):
            tstr = target.isoformat()
            candidates = [d for d in dates if d <= tstr]
            return candidates[-1] if candidates else None

        d_anchor = actual_date(anchor)
        d12      = actual_date(a12)
        d36      = actual_date(a36)
        d60      = actual_date(a60)

        result_ibov = {
            "cagr12": ibov_cagr(d12,  d_anchor, p12,  p_anchor) if d12  else None,
            "cagr36": ibov_cagr(d36,  d_anchor, p36,  p_anchor) if d36  else None,
            "cagr60": ibov_cagr(d60,  d_anchor, p60,  p_anchor) if d60  else None,
        }
        print(f"  IBOV 12M={result_ibov['cagr12']:.2f}% 36M={result_ibov['cagr36']:.2f}% 60M={result_ibov['cagr60']:.2f}%")
        return result_ibov
    except Exception as e:
        print(f"  ✗ IBOV fetch failed: {e}")
        return {"cagr12": None, "cagr36": None, "cagr60": None}


def update_history(anchor):
    """
    Atualiza o history.json com as cotas do mês atual.
    Lê o history.json existente, busca só o mês corrente na CVM,
    adiciona novas datas e recalcula correlações e drawdowns.
    """
    print(f"\n── Atualizando history.json")
    hist_path = Path(__file__).parent.parent / "docs" / "history.json"

    # Carregar histórico existente ou iniciar vazio
    if hist_path.exists():
        try:
            existing = json.loads(hist_path.read_text())
            # quotas[cnpjFmt][date] = quota
            quotas = {cnpj: dict(zip(fd["dates"], fd["quotas"]))
                      for cnpj, fd in existing.get("funds", {}).items()}
            print(f"  Histórico existente: {len(next(iter(quotas.values()), {}))} datas")
        except Exception as e:
            print(f"  Erro ao ler history.json: {e} — iniciando do zero")
            quotas = {f["cnpjFmt"]: {} for f in FUNDS}
    else:
        print("  history.json não encontrado — iniciando do zero")
        quotas = {f["cnpjFmt"]: {} for f in FUNDS}

    # Buscar mês atual (e anterior como fallback)
    months_to_fetch = set()
    months_to_fetch.add((anchor.year, anchor.month))
    # também mês anterior para garantir continuidade
    prev = anchor.replace(day=1) - datetime.timedelta(days=1)
    months_to_fetch.add((prev.year, prev.month))

    cnpj_map = {f["cnpj"]: f["cnpjFmt"] for f in FUNDS}
    cnpj_set = set(cnpj_map.keys())

    for year, month in sorted(months_to_fetch):
        data = fetch_monthly(year, month)
        if not data:
            continue
        added = 0
        for line in data["lines"][1:]:
            if not any(c in line for c in cnpj_set):
                continue
            cols = line.split(";")
            try:
                raw_cnpj = cols[data["col_date"] - 1].strip() if data["col_date"] > 0 else ""
                # encontrar CNPJ na linha
                found_cnpj = next((c for c in cnpj_set if c in line), None)
                if not found_cnpj:
                    continue
                cnpj_fmt = cnpj_map[found_cnpj]
                d = cols[data["col_date"]].strip()
                q = float(cols[data["col_quota"]].replace(",", "."))
                if d and d not in quotas.get(cnpj_fmt, {}):
                    quotas.setdefault(cnpj_fmt, {})[d] = q
                    added += 1
            except (ValueError, IndexError):
                continue
        print(f"  {year}-{month:02d}: {added} novas cotas adicionadas")

    # Janela de 3 anos a partir do anchor
    cutoff = subtract_months(anchor, 36).isoformat()

    # Datas comuns a todos os fundos dentro da janela
    all_date_sets = [set(d for d in quotas.get(f["cnpjFmt"], {}) if d >= cutoff)
                     for f in FUNDS]
    common_dates = sorted(set.intersection(*all_date_sets)) if all_date_sets else []

    if not common_dates:
        print("  Sem datas comuns — history.json não atualizado")
        return

    print(f"  Datas comuns: {len(common_dates)} ({common_dates[0]} → {common_dates[-1]})")

    # Retornos diários por fundo
    FUND_NOMES = {
        "22.232.927/0001-90": "Tarpon GT",
        "17.400.251/0001-66": "Organon",
        "18.302.338/0001-63": "Ártica Long Term",
        "37.495.383/0001-26": "Genoa Arpa",
        "42.698.666/0001-05": "Artax Ultra",
        "24.623.392/0001-03": "Guepardo Long Bias",
        "28.747.685/0001-53": "Kapitalo Tarkus",
        "10.500.884/0001-05": "Real Investor",
        "35.744.790/0001-02": "Schroder Tech L&S",
        "38.954.217/0001-03": "Pátria Long Biased",
        "32.073.525/0001-43": "Absolute Pace",
        "21.689.246/0001-92": "Arbor",
        "14.438.229/0001-17": "Charles River",
        "17.397.315/0001-17": "SPX Falcon",
    }

    returns_by_fund = {}
    for cnpj_fmt, nome in FUND_NOMES.items():
        qs = quotas.get(cnpj_fmt, {})
        rets = []
        for i in range(1, len(common_dates)):
            q0 = qs.get(common_dates[i-1])
            q1 = qs.get(common_dates[i])
            if q0 and q1:
                rets.append((q1 / q0) - 1)
            else:
                rets.append(0.0)
        returns_by_fund[cnpj_fmt] = rets

    # Correlação de Pearson
    def pearson(a, b):
        n = len(a)
        if n < 2:
            return 0.0
        ma, mb = sum(a)/n, sum(b)/n
        cov = sum((a[i]-ma)*(b[i]-mb) for i in range(n))
        sa  = math.sqrt(sum((x-ma)**2 for x in a))
        sb  = math.sqrt(sum((x-mb)**2 for x in b))
        return round(cov / (sa * sb), 4) if sa * sb > 0 else 0.0

    corr = {}
    cnpjs = list(FUND_NOMES.keys())
    for ca in cnpjs:
        corr[ca] = {cb: pearson(returns_by_fund[ca], returns_by_fund[cb]) for cb in cnpjs}

    # Drawdown máximo por fundo
    max_drawdowns = {}
    for cnpj_fmt, rets in returns_by_fund.items():
        cum, peak, max_dd = 1.0, 1.0, 0.0
        for r in rets:
            cum *= (1 + r)
            if cum > peak:
                peak = cum
            dd = (cum - peak) / peak
            if dd < max_dd:
                max_dd = dd
        max_drawdowns[cnpj_fmt] = round(max_dd * 100, 2)

    # Montar output
    funds_out = {}
    for cnpj_fmt, nome in FUND_NOMES.items():
        qs = quotas.get(cnpj_fmt, {})
        funds_out[cnpj_fmt] = {
            "nome":        nome,
            "dates":       common_dates,
            "quotas":      [qs.get(d, 0) for d in common_dates],
            "returns":     returns_by_fund[cnpj_fmt],
            "maxDrawdown": max_drawdowns[cnpj_fmt],
        }

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        common_dates[0],
        "to":          common_dates[-1],
        "commonDates": common_dates,
        "correlation": corr,
        "funds":       funds_out,
    }

    hist_path.write_text(json.dumps(output, ensure_ascii=False, separators=(',', ':')))
    size_kb = hist_path.stat().st_size / 1024
    print(f"  ✓ history.json atualizado ({size_kb:.0f} KB, {len(common_dates)} datas)")


def main():
    today = datetime.date.today()
    print(f"Running for {today.isoformat()}")

    anchor = find_anchor_date(today.year, today.month)
    a12 = subtract_months(anchor, 12)
    a36 = subtract_months(anchor, 36)
    a60 = subtract_months(anchor, 60)

    # ── Load previous maxQuota values from existing data.json ──
    out_path = Path(__file__).parent.parent / "docs" / "data.json"
    prev_max_quotas = {}
    if out_path.exists():
        try:
            prev_data = json.loads(out_path.read_text())
            for f in prev_data.get("funds", []):
                if f.get("cnpjFmt") and f.get("maxQuota"):
                    prev_max_quotas[f["cnpjFmt"]] = {
                        "maxQuota":     f["maxQuota"],
                        "maxQuotaDate": f.get("maxQuotaDate", ""),
                    }
            print(f"Loaded {len(prev_max_quotas)} previous max quotas from data.json")
        except Exception as e:
            print(f"Could not load previous data.json: {e}")

    results = [process_fund(f, anchor, prev_max_quotas) for f in FUNDS]

    print(f"\n── Ibovespa")
    ibov = fetch_ibov(anchor, a12, a36, a60)

    print(f"\n── CDI")
    cdi = fetch_cdi(anchor, a12, a36, a60)

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "anchorDate":  anchor.isoformat(),
        "ibov":        ibov,
        "cdi":         cdi,
        "funds":       results,
    }

    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n✓ Wrote {out_path} ({len(results)} funds)")

    # ── Atualizar history.json ──
    update_history(anchor)


if __name__ == "__main__":
    main()
