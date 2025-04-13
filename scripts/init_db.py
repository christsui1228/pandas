# scripts/init_db.py
import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from app.core.database import setup_database

print("Initializing database...")
try:
    setup_database()
    print("Database initialization completed successfully!")
except Exception as e:
    print(f"Database initialization failed: {e}")
