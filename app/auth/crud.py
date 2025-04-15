# CRUD operations for authentication models (User, Role, Permission)

from sqlmodel import Session, select
from pydantic import EmailStr
from app.core import security # Import security for password hashing
from datetime import datetime, timezone # Import datetime for timestamp

# Import models and security utilities correctly
# Adjust paths if your models/security are elsewhere
from .models import User, UserCreate, UserUpdate


def get_user_by_email(session: Session, *, email: EmailStr) -> User | None:
    """Get a user by email."""
    statement = select(User).where(User.email == email)
    return session.exec(statement).first()

def create_user(session: Session, *, user_create: UserCreate) -> User:
    """Create a new user, hashing the password."""
    hashed_password = security.get_password_hash(user_create.password)
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

def update_user(session: Session, *, db_user: User, user_in: UserUpdate) -> User | None:
    """Updates an existing user in the database."""
    # 获取 Pydantic 模型中已设置（非 None 或默认值）的字段及其值
    user_data = user_in.model_dump(exclude_unset=True)

    # 单独处理密码更新
    if "password" in user_data and user_data["password"]:
        hashed_password = security.get_password_hash(user_data["password"])
        db_user.hashed_password = hashed_password
        del user_data["password"]  # 从字典中移除，避免 setattr 直接设置明文密码

    # 更新其他提供的字段
    for key, value in user_data.items():
        setattr(db_user, key, value)

    # 更新时间戳
    db_user.updated_at = datetime.now(timezone.utc)

    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user

def delete_user_by_id(session: Session, *, user_id: int) -> bool:
    """Deletes a user from the database by their ID."""
    # Fetch the user first
    statement = select(User).where(User.id == user_id)
    user_to_delete = session.exec(statement).first()

    if user_to_delete:
        session.delete(user_to_delete)
        session.commit()
        return True  # Indicate successful deletion
    else:
        return False # Indicate user not found
