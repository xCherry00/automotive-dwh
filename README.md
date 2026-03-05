# Automotive DWH (PostgreSQL)

Projekt hurtowni danych dla sklepu z czesciami samochodowymi.

## Zakres
- Warstwy: `raw -> stg -> dwh -> marts`
- Zrodla dzienne: `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`
- Pipeline: extract, load do RAW, transform do STG, ladownie DWH (gwiazda), publikacja martow
- Data Quality: zestaw kontroli SQL

## Struktura
```text
automotive-dwh/
|-- data/
|   |-- incoming/
|   `-- archive/
|-- database/
|   |-- ddl/
|   `-- views/
|-- etl/
|-- tests/
|-- config.example.yaml
|-- docker-compose.yml
|-- generate_data.py
|-- main.py
`-- requirements.txt
```

## Model danych
### RAW
- `raw.customers`
- `raw.products`
- `raw.orders`
- `raw.order_items`

### STG
- `stg.customers_clean`
- `stg.products_clean`
- `stg.orders_clean`
- `stg.order_items_clean`

### DWH (model gwiazdy)
- Fakty: `dwh.fact_orders`, `dwh.fact_order_items`
- Wymiary: `dwh.dim_customer`, `dwh.dim_product`, `dwh.dim_date`, `dwh.dim_channel`, `dwh.dim_geography`

### MARTS
- `marts.daily_sales`
- `marts.top_parts`
- `marts.margin_by_category`
- `marts.cohort_retention`
- `marts.rfm_segments`

## Incremental loading
- RAW: dopisywanie batchy z `batch_date`
- STG: czyszczenie i deduplikacja per `batch_date` (row_number po kluczu biznesowym)
- DWH: UPSERT po kluczach naturalnych (`customer_nk`, `part_sku`, `order_nk`, `order_item_nk`)

## Generator danych (duza skala)
Skrypt generuje dane strumieniowo (bez trzymania calego datasetu w pamieci):

```bash
python generate_data.py --date 2026-02-26 --profile large
```

Profile:
- `dev`: 5k customers, 2k products, 20k orders
- `large`: 200k customers, 100k products, 1M orders
- `xlarge`: 1M customers, 250k products, 5M orders

Wlasna skala:
```bash
python generate_data.py --date 2026-02-26 --customers 300000 --products 150000 --orders 2000000 --min-items 1 --max-items 6
```


## Konfiguracja lokalna
1. Skopiuj plik przykladowy:
```bash
copy config.example.yaml config.yaml
```
2. Ustaw lokalne dane dostepowe do bazy w `config.yaml`.

`config.yaml` i lokalne wsady CSV sa ignorowane przez Git (`.gitignore`), aby nie publikowac hasel i danych roboczych.
## Uruchomienie
1. Start bazy:
```bash
docker-compose up -d
```
2. Instalacja zaleznosci:
```bash
pip install -r requirements.txt
```
3. Wygeneruj lub dostarcz dane wejsciowe:
```text
data/incoming/2026-02-26/customers.csv
data/incoming/2026-02-26/products.csv
data/incoming/2026-02-26/orders.csv
data/incoming/2026-02-26/order_items.csv
```
4. Pierwsze uruchomienie (z DDL):
```bash
python main.py --date 2026-02-26 --init-db
```
5. Kolejne uruchomienia dzienne:
```bash
python main.py --date 2026-02-27
```

## Testy jakosci
Zapytania kontrolne znajduja sie w `tests/dq_checks.sql`.

Przyklady:
- brak nulli w FK w faktach
- referencyjnosc pozycji
- duplikaty zamowien
- kontrola sum rabatow

## Rozszerzenia (kolejny krok)
- SCD2 dla `dim_customer` i `dim_product`
- bridge kompatybilnosci `bridge_product_compatibility`
- materialized views + harmonogram odswiezania
- orchestracja przez Airflow/Prefect

