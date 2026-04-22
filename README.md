## app_v2

这是一个**纯骨架** FastAPI 项目（无业务逻辑），用于：

- PostgreSQL + asyncpg + SQLAlchemy 2.0（全异步）
- pydantic-settings（敏感配置仅从环境变量读取，均无默认值）
- JWT 认证（python-jose + passlib[bcrypt]）
- 端点：`/api/v1/auth/register`、`/api/v1/auth/login`、`/api/v1/auth/me`
- Alembic（用户表 + 指标表基础迁移）
- 统一响应格式：`{ "code": 0, "data": ..., "message": "ok" }`

### 运行（本地）

1) 准备环境变量（参考 `.env.example`，务必自行填写）
2) 安装依赖

```bash
pip install -r app_v2/requirements.txt
```

3) 启动（示例）

```bash
uvicorn app.main:app --reload --app-dir app_v2
```

### 运行（Docker Compose）

```bash
docker compose -f app_v2/docker-compose.yml up --build
```

