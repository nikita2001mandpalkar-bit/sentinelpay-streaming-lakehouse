"""Database connection utilities for SentinelPay."""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def validate_database_config():
    """
    Validate required database environment variables.
    """

    required_configs = {
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
    }

    for key, value in required_configs.items():

        if value is None or value.strip() == "":

            raise ValueError(
                f"Missing required environment variable: {key}"
            )


def get_connection():
    """
    Create PostgreSQL connection.
    """

    validate_database_config()

    try:

        connection = psycopg2.connect(
            host="localhost",
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

        return connection

    except psycopg2.OperationalError:

        raise

    except psycopg2.DatabaseError:

        raise

    