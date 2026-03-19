FROM python:3.11

WORKDIR /app


COPY dbt/ /app/dbt/


RUN pip install dbt-core dbt-postgres


CMD ["dbt", "run", "--project-dir", "/app/dbt"]