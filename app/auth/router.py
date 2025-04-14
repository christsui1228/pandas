# app/routers/auth.py
from datetime import timedelta, datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import Session, select, SQLModel

from app.core.database import get_session
from app.core import security
from app.dependencies.auth import get_current_active_user, require_permission
from .models import User, Role, UserCreate, UserPublic, UserUpdate, UserPasswordUpdate
from .crud import get_user_by_email, create_user as crud_create_user, update_user as crud_update_user, delete_user_by_id as crud_delete_user_by_id

# Ensure Pydantic models are correctly defined or imported
class UserListResponse(UserPublic):
    pass # Or tailor fields as needed

# Re-add the Token model for the login response
class Token(SQLModel):
    access_token: str
    token_type: str = "bearer"

router = APIRouter(prefix="/auth", tags=["Authentication & Authorization"])

# --- 辅助函数：数据库查找用户 ---
def get_user_by_username(session: Session, username: str) -> User | None:
    """通过用户名获取用户"""
    statement = select(User).where(User.username == username)
    user = session.exec(statement).first()
    return user

def get_user_by_email(session: Session, email: str) -> User | None:
    """通过邮箱获取用户"""
    statement = select(User).where(User.email == email)
    user = session.exec(statement).first()
    return user


# --- 登录端点 ---
@router.post("/login/token", response_model=Token, tags=["Authentication"])
async def login_for_access_token(
    session: Session = Depends(get_session),
    form_data: OAuth2PasswordRequestForm = Depends()
):
    """
    用户登录以获取 JWT Bearer Token.

    使用表单数据 `username` 和 `password`.
    """
    user = get_user_by_username(session, form_data.username)
    if not user or not security.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="错误的用户名或密码",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(status_code=400, detail="非活动用户")

    # 可以自定义 token 过期时间，否则使用 security 文件中的默认值
    access_token_expires = timedelta(minutes=security.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        subject=user.id, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")

# --- 新增：创建用户端点 ---
@router.post("/users/", response_model=UserPublic, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_permission("user.create"))], tags=["Users"])
def create_user(
    *, # 强制后续参数为关键字参数
    session: Session = Depends(get_session),
    user_in: UserCreate
):
    """
    创建新用户。
    """
    # 检查用户名或邮箱是否已存在
    db_user_by_username = get_user_by_username(session, username=user_in.username)
    if db_user_by_username:
        raise HTTPException(
            status_code=400,
            detail="用户名已被注册",
        )
    db_user_by_email = get_user_by_email(session, email=user_in.email)
    if db_user_by_email:
         raise HTTPException(
            status_code=400,
            detail="邮箱已被注册",
        )

    # 创建用户对象
    hashed_password = security.get_password_hash(user_in.password)
    
    # 检查和处理角色ID
    role_id = user_in.role_id
    
    # 如果提供了角色 ID，验证它存在
    if role_id is not None:
        # 验证角色 ID 是否大于 0
        if role_id <= 0:
            raise HTTPException(
                status_code=400,
                detail="无效的角色 ID。角色 ID 必须大于 0。"
            )
            
        # 验证角色存在
        role_exists = session.exec(select(Role).where(Role.id == role_id)).first()
        if not role_exists:
            raise HTTPException(
                status_code=400,
                detail=f"角色 ID {role_id} 不存在。请提供有效的角色 ID。"
            )
    # 如果没有提供角色 ID，分配默认角色(普通用户)
    else:
        # 查询普通用户角色
        statement = select(Role).where(Role.name == "普通用户")
        default_role = session.exec(statement).first()
        if not default_role:
            # 如果找不到普通用户角色，尝试获取ID为2的角色
            statement = select(Role).where(Role.id == 2)
            default_role = session.exec(statement).first()
            
            # 如果还找不到，尝试获取任何有效角色
            if not default_role:
                default_role = session.exec(select(Role).order_by(Role.id)).first()
        
        if default_role:
            role_id = default_role.id
        else:
            # 如果仍然找不到角色，报错
            raise HTTPException(
                status_code=500,
                detail="无法创建用户：系统中没有可用的角色。请先创建角色。"
            )
    
    db_user = User(
        username=user_in.username,
        email=user_in.email,
        hashed_password=hashed_password,
        role_id=role_id  # 始终指定角色ID
    )
    # 默认新用户是激活状态 (is_active=True 在模型中定义)

    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user # FastAPI 会自动使用 UserRead schema 进行转换

# Endpoint to get current user's info - No specific permission code needed, just login
@router.get("/users/me", response_model=UserPublic, tags=["Users"])
async def read_users_me(
    current_user: User = Depends(get_current_active_user)
):
    """
    获取当前登录用户的信息。
    """
    return current_user

# Endpoint to list users - requires user.list permission
@router.get("/users/", response_model=List[UserPublic], dependencies=[Depends(require_permission("user.list"))], tags=["Users"])
async def read_users(
    session: Session = Depends(get_session),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100)
):
    """
    Retrieve users. Requires 'user.list' permission.
    """
    users = session.exec(select(User).offset(skip).limit(limit)).all()
    return users

# Endpoint to get a specific user by ID - requires user.read permission
@router.get("/users/{user_id}", response_model=UserPublic, dependencies=[Depends(require_permission("user.read"))], tags=["Users"])
def read_user(
    user_id: int,
    session: Session = Depends(get_session),
):
    """
    Get a specific user by id. Requires 'user.read' permission.
    """
    db_user = session.get(User, user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="未找到用户")
    return db_user

@router.patch("/users/{user_id}", response_model=UserPublic, dependencies=[Depends(require_permission("user.update"))], tags=["Users"])
def update_user(
    user_id: int,
    user_in: UserUpdate,
    session: Session = Depends(get_session),
):
    """
    Update a user. Requires 'user.update' permission.
    """
    db_user = session.get(User, user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="未找到用户")
    user = crud_update_user(session=session, db_user=db_user, user_in=user_in)
    return user

@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_permission("user.delete"))], tags=["Users"])
def delete_user(
    user_id: int,
    session: Session = Depends(get_session),
):
    """
    Delete a user. Requires 'user.delete' permission.
    """
    deleted = crud_delete_user_by_id(session=session, user_id=user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="未找到用户")
    # No content returned on successful deletion

# --- 新增：用户修改自己的密码 --- 
@router.patch(
    "/users/me/password", 
    status_code=status.HTTP_204_NO_CONTENT, 
    dependencies=[Depends(require_permission("user.change_password"))], 
    tags=["Users"]
)
def update_password_me(
    *, 
    session: Session = Depends(get_session),
    password_update: UserPasswordUpdate, 
    current_user: User = Depends(get_current_active_user) 
):
    """
    Update own password.
    """
    # 验证当前密码是否正确
    if not security.verify_password(password_update.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="当前密码不正确"
        )
    
    # 验证通过，哈希新密码并更新用户记录
    hashed_password = security.get_password_hash(password_update.new_password)
    current_user.hashed_password = hashed_password
    current_user.updated_at = datetime.now(timezone.utc)
    
    session.add(current_user)
    session.commit()
    # No need to refresh if we don't return the user object

    # Return nothing, FastAPI will send 204 No Content
