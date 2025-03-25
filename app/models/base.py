
from datetime import datetime, timezone as tz
from sqlmodel import SQLModel, Field

class TimestampMixin(SQLModel):
    """所有模型的基类，包含时间戳"""
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz.utc))