# app/core/security.py
# import os # No longer needed for getenv
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union

from jose import jwt, JWTError
from passlib.context import CryptContext
# from dotenv import load_dotenv # No longer needed

from .config import settings # Import the centralized settings

# load_dotenv() # Remove this line

# --- 配置 ---
# Values are now sourced from the settings object
# SECRET_KEY = os.getenv("SECRET_KEY", "your-default-secret-key-only-for-dev")
# ALGORITHM = os.getenv("ALGORITHM", "HS256")
# ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

# --- 密码处理 ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证明文密码与哈希密码是否匹配"""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """生成密码的哈希值"""
    return pwd_context.hash(password)

# --- JWT 处理 ---
def create_access_token(
    subject: Union[str, Any], expires_delta: Optional[timedelta] = None
) -> str:
    """
    创建 JWT Access Token

    :param subject: JWT的主题 (通常是用户ID或用户名)
    :param expires_delta: 可选的过期时间增量
    :return: JWT 字符串
    """
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES # Use settings
        )
    to_encode: Dict[str, Any] = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM) # Use settings
    return encoded_jwt

def decode_access_token(token: str) -> Optional[str]:
    """
    验证并解码 JWT Access Token

    :param token: JWT 字符串
    :return: 主题 (subject) 如果令牌有效且未过期，否则返回 None
    """
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]) # Use settings
        subject: str = payload.get("sub")
        if subject is None:
            return None # 或者抛出异常
        # 可选：在这里可以添加对令牌类型的检查 (payload.get("type") == "access")
        # 可选：在这里可以添加对令牌过期时间的更严格检查 (虽然 decode 会检查)
        return subject
    except JWTError: # 包括 ExpiredSignatureError 和其他 JWT 相关错误
        return None # 或者抛出特定的异常
    except Exception: # 捕获其他潜在错误
        return None
