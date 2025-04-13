import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# --- Add project root to sys.path ---
# This allows Alembic to find your app modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- End project root addition ---

# --- Import SQLModel and your models ---
from sqlmodel import SQLModel
from dotenv import load_dotenv

# --- Load environment variables for database URL ---
load_dotenv(os.path.join(project_root, '.env'))

# --- Import ALL your SQLModel models here! ---
# This is crucial for Alembic's autogenerate feature
# Example: from app.models.some_model import SomeModel
# Based on your database.py:
try:
    from app.models.sample_orders import SampleOrder
    from app.models.bulk_orders import BulkOrder
    # If you have a base Order model, import it too
    # from app.models import Order # Uncomment if exists
    from app.models.auth import User, Role, Permission, RolePermission
    print("Successfully imported models for Alembic.")
except ImportError as e:
    print(f"Error importing models for Alembic: {e}")
    print("Please ensure all SQLModel models are correctly placed and importable.")
    # Depending on your setup, you might need to adjust paths or ensure __init__.py files exist

# --- Alembic Config object ---
config = context.config

# --- Interpret the config file for Python logging ---
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Set target metadata ---
# For 'autogenerate' support, assign your SQLModel.metadata here
target_metadata = SQLModel.metadata

# --- Function to get Database URL from environment variables ---
def get_database_url():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Database connection details missing in environment variables.")
    # Use the same URL format as in your application
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=disable"

# --- Configure Alembic context ---
# Inject the database URL into the config object
# This avoids hardcoding it in alembic.ini
config.set_main_option('sqlalchemy.url', get_database_url())

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Use engine_from_config which reads from the config object
    # where we already placed the 'sqlalchemy.url'
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
