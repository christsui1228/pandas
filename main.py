from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session
import uvicorn
from datetime import datetime, date
# 导入数据库模型和配置
from app.core.database import get_session, get_engine
from app.auth.router import router as auth_router
from app.sample_orders.router import router as sample_orders_router
from app.customers.router import router as customers_router

# 获取数据库引擎
engine = get_engine()

# 创建FastAPI应用
app = FastAPI(title="Pandas API", description="样品和批量订单管理系统API")

# 配置CORS以允许前端访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，在生产环境中应该限制为特定域名
    allow_credentials=False,  # 设置为False当使用allow_origins=["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 包含认证路由 ---
app.include_router(auth_router, prefix="/api/v1", tags=["Authentication"])
app.include_router(sample_orders_router, prefix="/api/v1/sample-orders", tags=["Sample Orders"])
app.include_router(customers_router, prefix="/api/v1/customers", tags=["Customers"])

# 首页路由
@app.get("/")
async def root():
    return {"message": "Pandas API 服务运行中"}

# 应用程序入口点
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
