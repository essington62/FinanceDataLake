# CLAUDE.md — FinanceData Lake

Camada de **extração crua** (L1) de dados de mercado diários: yfinance + FRED → parquet local → S3 (`s3://equity-plataform/data_lake/`). Consumidor downstream: `crypto-market-state` (Kedro, modelo/scoring). Este repo **não** usa Kedro.

## Comandos

| Ação | Comando |
|---|---|
| Setup | `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt && cp .env.example .env` |
| FRED | `python scripts/download_fred.py` (requer `FRED_API_KEY` no env) |
| yfinance | `python scripts/download_yfinance.py` |
| Pipeline completo + sync S3 | `bash scripts/daily_update.sh` (cron 07:30 UTC) |
| Contagem do universo | `python3 -c "import yaml;c=yaml.safe_load(open('conf/tickers.yml'));print(len(c['fred']['series']),{k:len(v) for k,v in c['yfinance'].items()})"` |

Sem testes/lint configurados. Validar mudanças rodando o script individual e inspecionando o parquet gerado.

## Estrutura

| Path | Papel |
|---|---|
| `conf/tickers.yml` | Fonte única do universo: `global.start_date`, `fred.series[]` (`id`,`name`,`category`), `yfinance.indices[]` / `yfinance.assets[]` (`ticker`,`name`,`category`) |
| `scripts/lib/common.py` | IO compartilhado: `load_tickers`, `partition_path`, `read_partition`, `atomic_write_parquet` |
| `scripts/download_*.py` | Um script por fonte; incremental e idempotente |
| `scripts/daily_update.sh` | Orquestrador: carrega `.env`, roda FRED → yfinance (falha de um não aborta o outro), `aws s3 sync` |
| `data/`, `logs/` | Gitignored. `DATA_DIR` sobrescreve o root de dados via env |

## Layout de saída

| Fonte | Grupo | Diretório | Nome do arquivo | Schema |
|---|---|---|---|---|
| FRED | `fred.series` | `data/macro_daily/` | **`{id}.parquet`** (ex.: `DGS10.parquet`) | `date` (datetime64 UTC), `value` (float64) |
| yfinance | `indices` | `data/macro_daily/` | `{name}.parquet` | `date` UTC, `open`,`high`,`low`,`close`,`adj_close`,`volume` (float64) |
| yfinance | `assets` | `data/business_day/` | `{name}.parquet` | idem |

- Diretórios S3 espelham os locais (`business_day/`, `macro_daily/`).
- FRED usa `id`, yfinance usa `name` como filename — não unificar sem migrar S3 e o consumidor.
- `name` deve ser único dentro do diretório de destino (colisão sobrescreve arquivo).

## Contrato incremental (não quebrar)

1. Lê parquet existente → `next_start = max(date) + 1d` (ou `global.start_date` se vazio).
2. Baixa apenas o delta; yfinance usa `end = amanhã UTC` (exclusivo), FRED `end = hoje UTC`.
3. `concat` → coerção de tipos → `dropna` → `sort` → `drop_duplicates(date, keep="last")`.
4. Reescreve o arquivo inteiro via `atomic_write_parquet` (tmp + `os.replace`, snappy, `index=False`).
5. Falha por série/ticker loga `[FONTE ERROR|WARN]` e segue; nunca aborta o loop.

## Regras

- **Timezone:** todo `date` é `pd.to_datetime(..., utc=True)`. Datas de controle via `datetime.now(timezone.utc)`.
- **Escopo:** apenas extração crua diária. Proibido adicionar features, sinais, scoring, news/sentimento, intraday, CoinGlass/Binance, B3 (ver "Fora de escopo" no README).
- **Sem Kedro aqui**, mas mantenha a separação IO × transformação: helpers de IO em `lib/common.py`; funções de download/normalização puras sobre DataFrames.
- **Paths:** nada hardcoded fora de `PROJECT_ROOT`/`DATA_DIR`/`CONF_DIR`. Bucket S3 vive só em `daily_update.sh`.
- **Segredos:** `FRED_API_KEY` só via `.env`/env; AWS via `~/.aws/credentials`. Nunca commitar `.env`.
- **Rate limit yfinance:** manter `threads=False`, retry com backoff (`RETRY_SLEEP * attempt`) e `INTER_TICKER_SLEEP`.
- **Tickers de volatilidade** (`^VIX`, `^MOVE`, `^OVX`): valores ≤0/NaN em OHLC são ffill/bfill — preservar ao refatorar.
- **Deps:** versões pinadas em `requirements.txt`; atualizar explicitamente.

## Adicionar ticker/série

1. Incluir entrada em `conf/tickers.yml` no grupo correto, com `category` consistente com as existentes.
2. Rodar o script da fonte; a primeira carga faz backfill desde `global.start_date`.
3. Commit no padrão do histórico: `Adiciona <TICKERS> (<racional curto>)`.
4. Atualizar a contagem de tickers no `README.md` (seção Escopo) — usar o one-liner de contagem.
