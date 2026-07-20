# FinanceData Lake

Extração e carga diária de dados de mercado (yfinance + FRED) para o data
lake em S3 (`s3://equity-plataform/data_lake/`). Não contém modelo, scoring,
sinais de trading ou news — isso fica no projeto `crypto-market-state`.

## Escopo

- **Fontes:** yfinance (44 tickers: índices, commodities, ETFs, FX) e FRED (14 séries macro).
- **Cadência:** diária. Nenhuma rotina intraday.
- **Saída local:** `data/business_day/*.parquet` (ativos yfinance) e
  `data/macro_daily/*.parquet` (índices yfinance + séries FRED).
- **Saída S3:** `s3://equity-plataform/data_lake/business_day/` e
  `s3://equity-plataform/data_lake/macro_daily/`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # preencher FRED_API_KEY
aws configure          # credenciais AWS (ou ~/.aws/credentials já existente)
```

## Rodar

```bash
# ingestão individual
python scripts/download_fred.py
python scripts/download_yfinance.py

# orquestrador completo (ingestão + sync S3)
bash scripts/daily_update.sh
```

Cada script é incremental: lê o parquet existente em `data/`, baixa só o
delta desde a última data e reescreve o arquivo por completo (merge +
dedupe), sem depender de estado externo.

## Configuração

- `conf/tickers.yml` — lista de tickers/séries e `start_date` global.
- `.env` — `FRED_API_KEY` (nunca commitado).
- Credenciais AWS via `~/.aws/credentials` (padrão do boto3/AWS CLI) — não
  há chave hardcoded no código.

## Crontab esperado

```
30 7 * * *   scripts/daily_update.sh   # FRED + yfinance + sync S3
```

Horário único (07:30 UTC) porque as duas fontes rodam sequencialmente no
mesmo script — não há mais o split 07:00/07:35 de quando esta rotina
convivia com CoinGlass/Binance/news no projeto antigo.

## Fora de escopo (propositalmente)

- CoinGlass, Binance, qualquer periodicidade < 1 dia.
- Modelo/scoring/paper trading (`crypto-market-state`).
- News/sentimento (`cryptocompare`, `macro_news`, `fed_news`, `classify_news`, YouTube).
- B3 (fluxo estrangeiro via Bloomberg/calls institucionais; demais séries B3 fora do escopo por serem mensais).
- `smart_money.py` (motor de sinal de opções, não extração crua).
