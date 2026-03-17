#!/usr/bin/env python3
"""
Diagnóstico: verifica como o CNPJ do Tarpon aparece nos arquivos da CVM
em diferentes anos para identificar por que a inception está errada.
"""
import zipfile, io, urllib.request

CNPJ_RAW = "22232927000190"
CNPJ_FMT = "22.232.927/0001-90"

def fetch_and_search(year, month):
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month:02d}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
        lines = content.split("\n")
        header = lines[0]
        print(f"\n{year}-{month:02d} header: {header[:120]}")

        hits_raw = [l for l in lines if CNPJ_RAW in l]
        hits_fmt = [l for l in lines if CNPJ_FMT in l]
        print(f"  hits raw ({CNPJ_RAW}): {len(hits_raw)}")
        print(f"  hits fmt ({CNPJ_FMT}): {len(hits_fmt)}")
        if hits_raw:
            print(f"  sample raw: {hits_raw[0][:120]}")
        if hits_fmt:
            print(f"  sample fmt: {hits_fmt[0][:120]}")
        if not hits_raw and not hits_fmt:
            print(f"  *** NOT FOUND in this file ***")
    except Exception as e:
        print(f"\n{year}-{month:02d}: ERROR {e}")

# Test key years to find where the fund disappears
for year in [2026, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]:
    fetch_and_search(year, 6)
