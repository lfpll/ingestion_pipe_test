
FROM python:3.11-slim

WORKDIR /app

ENV PATH="/root/.local/bin:${PATH}"
COPY  /ingestion /app/ingestion
COPY main.py /app
COPY pyproject.toml poetry.lock /app/

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential

RUN curl -sSL https://install.python-poetry.org | python3 -

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

ENTRYPOINT ["poetry", "run", "python", "main.py"]
