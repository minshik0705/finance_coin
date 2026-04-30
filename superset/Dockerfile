FROM apache/superset:6.0.0

USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache \
    psycopg2-binary==2.9.10 \
    boto3 \
    "PyAthena[pandas]"

RUN /app/.venv/bin/python -c "import psycopg2, boto3, pyathena; print('driver import ok')"

USER superset
