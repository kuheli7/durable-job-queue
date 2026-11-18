FROM python:3.11-slim

WORKDIR /srv
COPY pyproject.toml ./
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
RUN pip install --no-cache-dir .

# The command is chosen per service in docker-compose.yml (api / worker / migrate); the partner has its own image.
