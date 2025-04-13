# CRUD operations for authentication models (User, Role, Permission)

from sqlmodel import Session, select
from typing import Optional
from pydantic import EmailStr

# Import models and security utilities correctly
# Adjust paths if your models/security are elsewhere
from app.models.auth import User, UserCreate, UserUpdate
from app.core.security import get_password_hash, verify_password # Make sure verify_password is here if needed


def get_user_by_email(session: Session, *, email: EmailStr) -> Optional[User]:
    """Get a user by email."""
    statement = select(User).where(User.email == email)
    return session.exec(statement).first()

def create_user(session: Session, *, user_create: UserCreate) -> User:
    """Create a new user, hashing the password."""
    hashed_password = get_password_hash(user_create.password)
    # Create user data dict excluding plain password, including hashed
    user_data = user_create.model_dump(exclude={'password'})
    user_data['hashed_password'] = hashed_password

    # Create User instance using model_validate for safety
    # Ensure all required fields in User model are present in user_data
    # or handle defaults appropriately
    db_user = User.model_validate(user_data)

    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user

# TODO: Add other CRUD functions (get_user, update_user, delete_user) here

# Placeholder for update_user
def update_user(session: Session, *, db_user: User, user_in: UserUpdate) -> User | None:
    """Placeholder for updating a user."""
    # Actual update logic will go here later
    # For now, just return the existing user or None if needed
    print(f"Placeholder: Would update user {db_user.id} with data {user_in}")
    # Example: Update fields from user_in
    # user_data = user_in.model_dump(exclude_unset=True)
    # for key, value in user_data.items():
    #     setattr(db_user, key, value)
    # if 'password' in user_data and user_in.password: # Example password update
    #     hashed_password = security.get_password_hash(user_in.password)
    #     db_user.hashed_password = hashed_password
    # session.add(db_user)
    # session.commit()
    # session.refresh(db_user)
    # return db_user
    pass # Or return db_user if the router expects the updated user
    return None # Returning None as a simple placeholder

# Placeholder for delete_user_by_id
def delete_user_by_id(session: Session, *, user_id: int) -> bool:
    """Placeholder for deleting a user by ID."""
    # Actual deletion logic will go here later
    print(f"Placeholder: Would delete user with ID {user_id}")
    # user = session.get(User, user_id)
    # if user:
    #     session.delete(user)
    #     session.commit()
    #     return True
    return False # Return False as a simple placeholder
