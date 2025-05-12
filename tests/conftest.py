# Pytest configuration file for fixtures
import pytest
from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer
import os

# Import your app's settings and models if needed for setup
# from app.core.config import settings # Assuming test settings might differ
# from app.models import * # Import all models that need to be created

# Option 1: Using Testcontainers for a real PostgreSQL instance (Recommended)
@pytest.fixture(scope="session")
def postgres_container():
    """Starts a PostgreSQL container for the test session."""
    # Ensure Docker is running
    if not os.system("docker info > /dev/null 2>&1") == 0:
        pytest.skip("Docker is not running. Skipping tests requiring PostgreSQL container.")

    with PostgresContainer("postgres:16") as postgres:
        # You might need to adjust settings based on container details if your app relies on them
        # For example, override settings.DATABASE_URL or individual components
        print(f"PostgreSQL container started at: {postgres.get_connection_url()}")
        yield postgres # Provide the container instance to the fixture

@pytest.fixture(scope="session")
def test_engine(postgres_container):
    """Creates a SQLAlchemy engine connected to the test PostgreSQL container."""
    # Use the connection URL provided by the container
    db_url = postgres_container.get_connection_url()
    engine = create_engine(db_url)
    # Optional: Create tables here if you want them session-scoped
    # SQLModel.metadata.create_all(engine)
    yield engine
    # Optional: Drop tables here if created session-scoped
    # SQLModel.metadata.drop_all(engine)

# Option 2: Using SQLite in-memory (Faster, but less realistic)
# @pytest.fixture(scope="session")
# def test_engine():
#     """Creates an in-memory SQLite engine for tests."""
#     engine = create_engine("sqlite:///:memory:")
#     SQLModel.metadata.create_all(engine) # Create tables once per session
#     yield engine
#     SQLModel.metadata.drop_all(engine) # Drop tables after session

@pytest.fixture(scope="function")
def db_session(test_engine):
    """Provides a clean database session for each test function."""
    # Create tables before each test function to ensure isolation
    SQLModel.metadata.create_all(test_engine)
    
    # Use sessionmaker for proper transaction handling
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = SessionLocal()
    
    try:
        yield session # Provide the session to the test
    finally:
        session.close() # Close the session
        # Drop tables after each test function for complete isolation
        SQLModel.metadata.drop_all(test_engine)

# You might need fixtures to create pre-requisite data (e.g., a default user)
# @pytest.fixture
# def default_user(db_session: Session):
#     user = User(username="testuser", ...) # Create user model instance
#     db_session.add(user)
#     db_session.commit()
#     db_session.refresh(user)
#     return user 