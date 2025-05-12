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

# --- Import centralized settings ---
from app.core.config import settings # Import the centralized settings

# --- Import ALL your SQLModel models here! ---
# This is crucial for Alembic's autogenerate feature
try:
    # Import from the new feature-based structure for most models
    from app.sample_orders.models import SampleOrder
    from app.customers.models import SampleCustomer, BulkCustomer, CustomerFollowUp
    from app.auth.models import User, Role, Permission # Assuming RolePermission is a link table

    # Import from the central app/models/ directory for specific models
    from app.models.bulk_orders import BulkOrder
    from app.models.orders import Order

    # Add imports for any other SQLModel tables you have defined

    print("Successfully imported models for Alembic.")
except ImportError as e:
    print(f"Error importing models for Alembic: {e}")
    print("Please ensure all SQLModel models are correctly placed and importable.")
    # print("Verify paths like 'app.bulk_orders.models', 'app.original_orders.models', etc.")
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

# --- Function to get Database URL from centralized settings ---
def get_database_url_from_settings():
    # Construct the database URL from the settings object
    # Ensure your Settings model has these attributes:
    # DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    url = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    # Add sslmode=disable if it's consistently used, or make it configurable in settings
    # For example, if settings has DB_SSL_MODE:
    # url = f"{url}?sslmode={settings.DB_SSL_MODE}"
    # For now, assuming sslmode=disable is always desired for migrations based on original code
    return f"{url}?sslmode=disable"

# --- Configure Alembic context ---
# Inject the database URL into the config object
# This avoids hardcoding it in alembic.ini
config.set_main_option('sqlalchemy.url', get_database_url_from_settings())

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
