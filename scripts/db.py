import os
from dataclasses import dataclass

import psycopg2
from psycopg2.extensions import connection as PgConnection


@dataclass(frozen=True)
class PostgresConfig:
    """
    Small config object for PostgreSQL connection settings.

    I keep these values in environment variables so I don't hardcode secrets
    (like passwords) into the repo.
    """

    host: str
    port: int
    dbname: str
    user: str
    password: str


def load_postgres_config() -> PostgresConfig:
    """
    Load PostgreSQL connection settings from environment variables.

    Required:
    - PGHOST
    - PGPORT
    - PGDATABASE
    - PGUSER
    - PGPASSWORD
    """
    missing = [k for k in ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"] if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            "Missing required environment variables for PostgreSQL connection: "
            + ", ".join(missing)
            + "\n\nExample (PowerShell):\n"
            + '$env:PGHOST="localhost"\n'
            + '$env:PGPORT="5432"\n'
            + '$env:PGDATABASE="crypto_db"\n'
            + '$env:PGUSER="postgres"\n'
            + '$env:PGPASSWORD="your_password"\n'
        )

    return PostgresConfig(
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )


def get_connection(config: PostgresConfig | None = None) -> PgConnection:
    """
    Create and return a new psycopg2 connection.

    Each ETL run should:
    - open a connection
    - do its inserts
    - commit
    - close the connection
    """
    if config is None:
        config = load_postgres_config()

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )

