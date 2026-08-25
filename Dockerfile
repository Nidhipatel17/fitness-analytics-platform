# The ingestion service: extract.py, load_raw.py, and the quality checks --
# deliberately not dbt or Airflow, which already have their own execution
# context in docker-compose.yml. One purpose per image.
#
# No fixed action baked in: this is meant to be invoked with an explicit
# command, e.g. as a Cloud Run Job, a Kubernetes CronJob, or Airflow's
# DockerOperator -- not run bare with no arguments in production.
#
#   docker build -t fitness-ingestion .
#   docker run --rm \
#     -v ~/.config/gcloud:/root/.config/gcloud:ro \
#     fitness-ingestion ingestion/extract.py --source synthetic --entity all
#   docker run --rm ... fitness-ingestion ingestion/load_raw.py --project <id> ...
#   docker run --rm ... fitness-ingestion quality/freshness_check.py --project <id>

FROM python:3.11-slim

WORKDIR /app

COPY requirements-ingestion.txt .
RUN pip install --no-cache-dir -r requirements-ingestion.txt

COPY ingestion/ ingestion/
COPY quality/ quality/
COPY scripts/ scripts/

ENTRYPOINT ["python3"]
CMD ["ingestion/extract.py", "--help"]
