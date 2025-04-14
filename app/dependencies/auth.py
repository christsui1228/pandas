# app/dependencies/auth.py
from typing import Generator, Optional, Callable

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlmodel import Session, select

from app.core.database import get_session
from app.core import security # 从我们刚创建的文件导入
from app.auth.models import User, Role, Permission # 从模型文件导入

# OAuth2 方案，"/api/v1/login/token" 是我们之后会创建的登录端点的路径
# tokenUrl 指向获取 token 的 API 路径
# Correct the tokenUrl to match the actual router path
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login/token")

def get_current_user(
    session: Session = Depends(get_session), token: str = Depends(oauth2_scheme)
) -> User:
    """
    依赖项：从 token 获取当前用户
    1. 解码 token 获取用户 ID (subject)
    2. 从数据库查询用户
    3. 验证用户是否存在且激活
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无法验证凭证",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    # 打印token用于调试
    print(f"DEBUG - 收到的令牌: {token[:15]}...")
    
    subject = security.decode_access_token(token)
    if subject is None:
        print("DEBUG - 令牌解码失败，subject 为 None")
        raise credentials_exception

    # 假设 subject 存储的是 user_id
    try:
        user_id = int(subject)
    except ValueError:
        raise credentials_exception # subject 不是有效的整数 ID

    statement = select(User).where(User.id == user_id)
    user = session.exec(statement).first()

    if user is None:
        raise credentials_exception
    if not user.is_active:
        raise HTTPException(status_code=400, detail="非活动用户")

    return user

def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """
    依赖项：获取当前活动用户 (只是对 get_current_user 的简单包装，确保显式检查激活状态)
    虽然 get_current_user 内部已经检查了 is_active，但有时多一层显式依赖更清晰
    """
    # get_current_user 内部已检查 is_active
    return current_user

# 可选：获取当前超级用户的依赖
def get_current_active_superuser(
    current_user: User = Depends(get_current_user),
) -> User:
    """
    依赖项：获取当前活动的超级用户
    """
    if not current_user.role or current_user.role.name != "super_admin": # 假设超级管理员角色名为 "super_admin"
        raise HTTPException(
            status_code=403, detail="需要超级管理员权限"
        )
    if not current_user.is_active: # 双重检查
         raise HTTPException(status_code=400, detail="非活动用户")
    return current_user

def require_permission(required_permission_code: str) -> Callable:
    """
    FastAPI dependency that checks if the current user has the required permission.

    Args:
        required_permission_code: The code of the permission required (e.g., "user.create").

    Raises:
        HTTPException(403): If the user does not have the required permission.

    Returns:
        A dependency function that performs the check.
    """
    def permission_checker(current_user: User = Depends(get_current_active_user)) -> User:
        # Superusers bypass permission checks
        if current_user.is_superuser:
            return current_user

        if not current_user.role or not current_user.role.permissions:
            # If user has no role or role has no permissions assigned
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足或角色未分配权限"
            )

        # Check if any of the user's role permissions match the required code
        has_permission = any(
            permission.code == required_permission_code
            for permission in current_user.role.permissions
        )

        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"权限不足，需要 '{required_permission_code}' 权限"
            )
        
        # If permission check passes, return the user object for potential further use
        return current_user

    return permission_checker
