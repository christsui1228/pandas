#!/usr/bin/env python
# scripts/init_roles.py
import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from sqlmodel import Session, select
from app.core.database import get_engine
from app.models.auth import Role, Permission, RolePermission, User, UserCreate
from app.crud.auth import get_user_by_email, create_user as crud_create_user

# Define the 5 roles based on user specification
ROLES_DATA = [
    {"id": 1, "level": 0, "name": "super_admin", "description": "系统超级管理员，具备所有权限"},
    {"id": 2, "level": 1, "name": "admin", "description": "管理员，具有系统管理权限"},
    {"id": 3, "level": 20, "name": "team_leader", "description": "组长，包含售前的权限，可以管理售前员工"},
    {"id": 4, "level": 30, "name": "pre_sales", "description": "售前，具有创建编辑客户的权限，无删除权限"},
    {"id": 5, "level": 40, "name": "intern", "description": "实习阶段，只有查看的权限"} # Changed level from 20 to 40 for uniqueness
]

# Define the full set of permissions based on user specification
PERMISSIONS_DATA = [
    # System Management
    {"id": 1, "name": "用户管理", "code": "system.manage_users", "description": "管理系统用户，包括创建、修改、删除用户"},
    {"id": 2, "name": "角色管理", "code": "system.manage_roles", "description": "管理系统角色，包括创建、修改、删除角色"},
    {"id": 3, "name": "权限管理", "code": "system.manage_permissions", "description": "管理系统权限，包括分配权限到角色"},
    {"id": 4, "name": "查看日志", "code": "system.view_logs", "description": "查看系统操作日志"},
    # User Specific Permissions
    {"id": 5, "name": "创建用户", "code": "user.create", "description": "创建新用户"},
    {"id": 6, "name": "查看用户", "code": "user.read", "description": "查看用户信息"},
    {"id": 7, "name": "更新用户", "code": "user.update", "description": "更新用户信息"},
    {"id": 8, "name": "删除用户", "code": "user.delete", "description": "删除用户"},
    {"id": 9, "name": "列出用户", "code": "user.list", "description": "列出所有用户"},
    {"id": 10, "name": "修改密码", "code": "user.change_password", "description": "修改用户密码"},
    {"id": 11, "name": "分配角色", "code": "user.assign_roles", "description": "为用户分配角色"},
    # Role Specific Permissions
    {"id": 12, "name": "创建角色", "code": "role.create", "description": "创建新角色"},
    {"id": 13, "name": "查看角色", "code": "role.read", "description": "查看角色信息"},
    {"id": 14, "name": "更新角色", "code": "role.update", "description": "更新角色信息"},
    {"id": 15, "name": "删除角色", "code": "role.delete", "description": "删除角色"},
    {"id": 16, "name": "分配权限", "code": "role.assign_permissions", "description": "为角色分配权限"},
    # Customer Specific Permissions (assuming 'invoice' was a typo for 'customer')
    {"id": 17, "name": "创建客户", "code": "customer.create", "description": "创建新客户"},
    {"id": 18, "name": "查看客户", "code": "customer.read", "description": "查看客户信息"},
    {"id": 19, "name": "更新客户", "code": "customer.update", "description": "更新客户信息"},
    {"id": 20, "name": "删除客户", "code": "customer.delete", "description": "删除客户"},
    {"id": 21, "name": "列出客户", "code": "customer.list", "description": "列出所有客户"},
    # Data Specific Permissions
    {"id": 22, "name": "导入数据", "code": "data.upload", "description": "上传新的表单信息"},
    {"id": 23, "name": "更新数据", "code": "data.refresh", "description": "数据同步更新"}
]

# Define which permissions each role gets based on user's role_permissions table
ROLE_PERMISSIONS = {
    "super_admin": [ p["code"] for p in PERMISSIONS_DATA ], # IDs: 1-23
    "admin": [ "user.read", "user.change_password", "customer.read", "customer.update", "customer.list" ], # IDs: 6, 10, 18, 19, 21
    "team_leader": [ "user.read", "user.change_password", "customer.read", "customer.update", "customer.list" ], # IDs: 6, 10, 18, 19, 21
    "pre_sales": [ "customer.create", "customer.read", "customer.update", "customer.list", "data.upload" ], # IDs: 17, 18, 19, 21, 22
    "intern": [ "customer.read", "customer.update", "customer.list" ] # IDs: 18, 19, 21
}

# 创建基本角色和权限
def init_roles_and_permissions():
    engine = get_engine()
    
    with Session(engine) as session:
        # --- Check if roles already exist ---
        existing_role = session.exec(select(Role).where(Role.id == 1)).first()
        if existing_role:
            print(f"角色 '{existing_role.name}' (ID: 1) 已存在，跳过初始化。")
            return

        print("开始初始化角色和权限...")

        # --- Create Permissions ---
        permissions_map = {}
        for perm_data in PERMISSIONS_DATA:
            permission = Permission(**perm_data)
            session.add(permission)
            session.commit() # Commit permissions to get IDs
            session.refresh(permission)
            permissions_map[perm_data["code"]] = permission
            print(f"  创建权限: {permission.name} ({permission.code})")

        # --- Create Roles ---
        roles_map = {}
        for role_data in ROLES_DATA:
            # We directly set the ID based on the spec, but ensure it's clean first
            # For created_by_id, we set it to None initially
            role = Role(
                id=role_data["id"],
                level=role_data["level"],
                name=role_data["name"],
                description=role_data["description"],
                created_by_id=None # System initialized
            )
            session.add(role)
            session.commit() # Commit roles to get IDs and ensure they exist for FKs
            session.refresh(role)
            roles_map[role_data["name"]] = role
            print(f"  创建角色: {role.name} (ID: {role.id}, Level: {role.level})")

        # --- Assign Permissions to Roles ---
        print("开始分配权限给角色...")
        for role_name, perm_codes in ROLE_PERMISSIONS.items():
            role = roles_map.get(role_name)
            if not role:
                print(f"警告: 角色 '{role_name}' 未找到，无法分配权限。")
                continue
            print(f"  分配给 '{role_name}':")
            for code in perm_codes:
                permission = permissions_map.get(code)
                if permission:
                    role_permission_link = RolePermission(role_id=role.id, permission_id=permission.id)
                    session.add(role_permission_link)
                    print(f"    - {permission.name} ({permission.code})")
                else:
                    print(f"    警告: 权限代码 '{code}' 未找到，无法分配给 '{role_name}'。")
            session.commit() # Commit assignments for the role

        print("\n角色和权限初始化完成！")

if __name__ == "__main__":
    init_roles_and_permissions()

    # --- 检查并创建超级管理员用户 ---
    print("检查并创建超级管理员用户...")
    engine = get_engine() # Re-get engine or pass from above
    SUPERUSER_EMAIL = "admin@example.com" # Default Superuser Email
    SUPERUSER_USERNAME = "superuser"      # Default Superuser Username
    SUPERUSER_PASSWORD = "abc123456"    # Default Superuser Password

    with Session(engine) as session:
        user = get_user_by_email(session=session, email=SUPERUSER_EMAIL)
        if not user:
            print(f"未找到超级管理员用户 ({SUPERUSER_EMAIL})，正在创建...")
            user_in = UserCreate(
                username=SUPERUSER_USERNAME,
                email=SUPERUSER_EMAIL,
                password=SUPERUSER_PASSWORD
                # role_id is not typically in UserCreate, will be set after creation
            )
            try:
                # crud_create_user handles password hashing
                user = crud_create_user(session=session, user_create=user_in)

                # Explicitly set superuser status and role AFTER initial creation
                user.is_superuser = True
                user.role_id = 1 # Assuming super_admin role ID is 1 (defined in ROLES_DATA)
                session.add(user) # Add again to stage changes
                session.commit()
                session.refresh(user)
                print(f"超级管理员用户 '{user.username}' ({user.email}) 创建成功。")
            except Exception as e:
                print(f"创建超级管理员用户时出错: {e}")
                session.rollback() # Rollback on error
        else:
            print(f"超级管理员用户 ({SUPERUSER_EMAIL}) 已存在。")
            # --- Add logic to check and update existing superuser --- 
            updated = False
            if not user.is_superuser:
                user.is_superuser = True
                updated = True
                print("  - 设置 is_superuser = True")
            # Assuming super_admin role ID is 1, check ROLE_DATA if unsure
            if user.role_id != 1:
                user.role_id = 1
                updated = True
                print("  - 设置 role_id = 1")

            if updated:
                try:
                    session.add(user)
                    session.commit()
                    print("  - 更新已提交。")
                except Exception as e:
                    print(f"  - 更新超级管理员用户时出错: {e}")
                    session.rollback()
            # --- End of update logic ---

    print("初始化脚本完成。")
