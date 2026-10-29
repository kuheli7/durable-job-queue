from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.db import make_engine, make_session_factory


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    """Real Postgres (docker compose up -d db). The schema comes from the real Alembic migration."""
    command.upgrade(Config("alembic.ini"), "head")
    eng = make_engine(Settings.from_env().database_url)
    yield eng
    eng.dispose()


@pytest.fixture()
def session_factory(engine: Engine) -> Iterator[sessionmaker[Session]]:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE jobs"))
    yield make_session_factory(engine)


@pytest.fixture()
def session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    with session_factory() as s:
        yield s
