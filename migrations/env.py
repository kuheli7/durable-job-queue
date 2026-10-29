from alembic import context
from sqlalchemy import create_engine

from app.config import Settings

config = context.config


def run_migrations_online() -> None:
    engine = create_engine(Settings.from_env().database_url)
    with engine.connect() as connection:
        context.configure(connection=connection, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
