from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def make_engine(database_url: str) -> Engine:
    # pool_pre_ping: transparently replace connections that Postgres closed (restart, failover)
    return create_engine(database_url, pool_size=5, max_overflow=5, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    # expire_on_commit=False: rows stay readable after the commit every queue function does
    return sessionmaker(engine, expire_on_commit=False)
