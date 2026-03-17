#!/usr/bin/env python3
"""
Diagnóstico: verifica as primeiras e últimas cotas do Ártica
nos arquivos históricos para entender o que está sendo capturado.
"""
import zipfile, io, urllib.request

CNPJ_RAW = "18302338000163"
CNPJ_FMT = "18.302.338/0001-63"

def fetch_and_inspect(year, month=None):
    if month:
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month:02d}.zip"
        label = f"{year}-{month:02d}"
    else:
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
        label = f"{year} (anual)"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            content = zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")

        lines = content.split("\n")
        header = lines[0]
        cols = [h.strip() for h in header.split(";")]

        hits = [l for l in lines if CNPJ_RAW in l or CNPJ_FMT in l]

        print(f"\n{label}: {len(hits)} linhas encontradas")
        if hits:
            print(f"  Primeira: {hits[0][:150]}")
            print(f"  Última:   {hits[-1][:150]}")
            # Parse date and quota columns
            col_date  = cols.index("DT_COMPTC") if "DT_COMPTC" in cols else -1
            col_quota = cols.index("VL_QUOTA")  if "VL_QUOTA"  in cols else -1
            if col_date >= 0 and col_quota >= 0:
                first_cols = hits[0].split(";")
                last_cols  = hits[-1].split(";")
                print(f"  Primeira cota: {first_cols[col_date].strip()} → R$ {first_cols[col_quota].strip()}")
                print(f"  Última cota:   {last_cols[col_date].strip()} → R$ {last_cols[col_quota].strip()}")
        else:
            print(f"  NÃO ENCONTRADO")
    except Exception as e:
        print(f"\n{label}: ERRO — {e}")

# Arquivos históricos anuais
for year in [2019, 2020]:
    fetch_and_inspect(year)

# Arquivos mensais do início
for month in [1, 2, 3]:
    fetch_and_inspect(2021, month)
