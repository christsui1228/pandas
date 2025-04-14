from typing import List, Optional, TYPE_CHECKING
from pydantic import EmailStr
from sqlmodel import Field, Relationship, SQLModel
from datetime import datetime

if TYPE_CHECKING:
    from .auth import User, Role


# --- Link Tables ---
class RolePermission(SQLModel, table=True):
    """Link table for many-to-many relationship between Role and Permission"""
    __tablename__ = "role_permissions"
    
    role_id: int = Field(foreign_key="roles.id", primary_key=True)
    permission_id: int = Field(foreign_key="permissions.id", primary_key=True)


# --- Main Models ---
class Permission(SQLModel, table=True):
    __tablename__ = "permissions"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    code: str = Field(index=True, unique=True, max_length=100)
    description: Optional[str] = Field(default=None, max_length=255)

    # Relationships
    roles: List["Role"] = Relationship(back_populates="permissions", link_model=RolePermission)


class Role(SQLModel, table=True):
    __tablename__ = "roles"
    id: Optional[int] = Field(default=None, primary_key=True)
    level: int = Field(index=True)
    name: str = Field(index=True, unique=True, max_length=50)
    description: Optional[str] = Field(default=None, max_length=255)
    created_by_id: Optional[int] = Field(default=None, foreign_key="users.id")

    # Relationships
    permissions: List["Permission"] = Relationship(back_populates="roles", link_model=RolePermission)
    users: List["User"] = Relationship(
        back_populates="role",
        sa_relationship_kwargs={"foreign_keys": "[User.role_id]"}
    )
    created_by: Optional["User"] = Relationship(
        back_populates="created_roles",
        sa_relationship_kwargs={"foreign_keys": "[Role.created_by_id]"}
    )


class User(SQLModel, table=True):
    __tablename__ = "users"
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, max_length=50)
    email: str = Field(index=True, unique=True, max_length=100)
    hashed_password: str = Field()
    is_active: bool = Field(default=True)
    is_superuser: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, sa_column_kwargs={"onupdate": datetime.now})
    role_id: Optional[int] = Field(default=None, foreign_key="roles.id")
    created_by_id: Optional[int] = Field(default=None, foreign_key="users.id")

    # --- Relationships ---
    role: Optional["Role"] = Relationship(
        back_populates="users",
        sa_relationship_kwargs={"foreign_keys": "[User.role_id]"}
    )
    created_roles: List["Role"] = Relationship(
        back_populates="created_by",
        sa_relationship_kwargs={"foreign_keys": "[Role.created_by_id]"}
    )

    creator: Optional["User"] = Relationship(
        back_populates="created_users",
        sa_relationship_kwargs=dict(
            remote_side="User.id",
            foreign_keys="[User.created_by_id]",
            primaryjoin="User.created_by_id==User.id",
        ),
    )
    created_users: List["User"] = Relationship(back_populates="creator")


# --- Pydantic models for API requests/responses ---
# Model for creating a user (API input)
class UserCreate(SQLModel):
    username: str = Field(index=True)
    email: EmailStr = Field(unique=True, index=True)
    password: str
    role_id: Optional[int] = Field(default=None, foreign_key="roles.id")

# Model for reading user data (API output - public)
class UserPublic(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, max_length=50)
    email: str = Field(index=True, unique=True, max_length=100)
    is_active: bool = Field(default=True)

# Model for updating a user (API input)
class UserUpdate(SQLModel):
    username: Optional[str] = None
    email: Optional[str] = None
    is_active: Optional[bool] = None

# Model for updating user password (used by user themselves)
class UserPasswordUpdate(SQLModel):
    current_password: str
    new_password: str
