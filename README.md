# Currency Exchange ETL

An Apache Airflow project that fetches daily EUR exchange rates for USD, GBP,
and JPY from the Frankfurter API and upserts them into MongoDB.

## Run locally

1. Copy `.env.example` to `.env` and set strong MongoDB and Mongo Express passwords.
2. Start the project with `astro dev start`.
3. Open Airflow at <http://localhost:8080> and enable the `currency_exchange_etl` DAG.
4. Stop the services with `astro dev stop`.

The DAG reads `MONGO_URI`, `MONGO_DB`, and `MONGO_COLLECTION` from the
environment. The Astro CLI loads values from `.env` for local development.
Mongo Express is available at <http://localhost:8081>.

## Project files

- `dags/currency_exchange_etl.py`: extracts, transforms, and stores exchange rates.
- `docker-compose.override.yml`: adds MongoDB and Mongo Express to the local stack.
- `requirements.txt`: Python dependencies for the DAG.
- `tests/`: DAG import and configuration checks.

Local credentials, virtual environments, and generated Airflow state are excluded
from Git. Keep `.env` private and configure equivalent variables in deployment.
