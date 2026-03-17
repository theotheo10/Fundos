# Ranking de Fundos — CAGR

Site estático hospedado no GitHub Pages que exibe o CAGR de fundos de investimento em diferentes horizontes, com dados buscados diariamente da CVM.

## Como funciona

```
scripts/fetch_data.py   ← roda via GitHub Actions (seg–sex, 21h UTC)
        │
        └─ busca ZIPs da CVM, calcula CAGR, escreve docs/data.json
                                                        │
                                              docs/index.html lê o JSON
                                              e renderiza a tabela
```

- **Fonte de dados:** [CVM / inf_diario_fi](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/)
- **Hospedagem:** GitHub Pages (pasta `docs/`)
- **Atualização:** GitHub Actions, segunda a sexta às 21h UTC (18h BRT)

## Setup (primeira vez)

### 1. Criar o repositório

```bash
git init
git add .
git commit -m "init"
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
git push -u origin main
```

### 2. Ativar GitHub Pages

1. Vá em **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: `main` · pasta: `/docs`
4. Salve — o site ficará em `https://SEU_USUARIO.github.io/SEU_REPO/`

### 3. Rodar o Action pela primeira vez

1. Vá em **Actions → Update fund data & deploy**
2. Clique em **Run workflow**
3. Aguarde ~5 minutos (a CVM tem arquivos grandes)

Após a primeira execução o `docs/data.json` será populado e o site exibirá os dados.

## Adicionar ou remover fundos

Edite a lista `FUNDS` em `scripts/fetch_data.py` e em `docs/index.html` (apenas o array `FUNDS` no JS, usado para o skeleton inicial). Faça push — o próximo agendamento usará a lista nova.

## Estrutura

```
.
├── .github/
│   └── workflows/
│       └── update.yml       # GitHub Actions: fetch + deploy
├── docs/
│   ├── index.html           # Site estático
│   └── data.json            # Gerado automaticamente pelo Action
└── scripts/
    └── fetch_data.py        # Script Python de coleta
```

## CAGR calculado

| Coluna | Horizonte | Anos usados |
|---|---|---|
| CAGR 12M | Cota de hoje vs. cota de 12 meses atrás | 1 |
| CAGR 36M | Cota de hoje vs. cota de 36 meses atrás | 3 |
| CAGR 60M | Cota de hoje vs. cota de 60 meses atrás | 5 |
| CAGR desde início | Cota de hoje vs. primeira cota disponível | anos exatos |
