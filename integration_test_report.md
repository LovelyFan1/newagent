# 前后端一体化（单端口）集成测试报告

## 测试环境
- 后端: `app_v2` Docker Compose
- 访问入口: `http://127.0.0.1:8000/`（根路径重定向到 `/web/login.html`）
- 静态挂载: `/web/*` -> `app_v2/web/`
- API: `/api/v1/*`（同域同端口）

## 关键部署变更
- 已将前端 `web/` 完整移植到 `app_v2/web/` 并随镜像构建复制到容器 `/app/web`
- `app_v2/app/main.py`:
  - `app.mount("/web", StaticFiles(directory="web", html=True), name="web")`
  - `GET /` -> 重定向 `/web/login.html`
- 前端 `API_BASE_URL` 已改为同源相对路径（`''`），无需跨域

## 端到端测试用例结果
| 测试项 | 操作 | 预期 | 结果 |
| :--- | :--- | :--- | :--- |
| 页面访问 | 打开根路径 `/` | 跳转到登录页，背景正常 | 通过（307 -> `/web/login.html`，200） |
| 静态资源 | 打开登录/主页面 | three/echarts/js 无 404 | 通过（关键资源均 200） |
| 注册 | 登录页输入账号/密码点注册 | 注册成功，自动进入主页面 | 通过（API 200） |
| 登录 | 使用账号密码登录 | 进入主页面，显示用户信息 | 通过（API 200，`/auth/me` 200） |
| 评分查询 | 输入“比亚迪 2022 年评分” | 展示真实评分（总分/评级/维度） | 通过（`/scoring/比亚迪?year=2022` 200） |
| Agent 分析 | 输入“分析比亚迪 2022 年财务风险” | 返回结构化报告并渲染图表 | 通过（`status=completed`） |
| 澄清机制 | 输入“比亚迪的营收” | 返回澄清问题 | 通过（`status=needs_clarification`） |
| 退出登录 | 点击退出按钮 | 清理 token 并回登录页 | 通过 |
| 文件上传 | 拖拽/选择文件上传 | 若后端未实现，前端提示不可用 | 通过（前端明确提示“未开放接口”） |

## 自动化验证摘要
- 根路径与静态页面:
  - `/` -> 307 Location: `/web/login.html`
  - `/web/login.html` -> 200
  - `/web/index.html` -> 200
- 关键静态资源（示例）:
  - `/web/vendor/three.min.js` -> 200
  - `/web/vendor/echarts.min.js` -> 200
  - `/web/js/api-client.js` -> 200
  - `/web/js/app.js` -> 200
- API 全链路:
  - register/login/me/scoring/agent completed/agent clarification 全部 200

## 结论
已完成前端文件移植与后端静态挂载，前后端同域同端口一体化部署已通过端到端测试，**系统可单端口访问并可对外演示**。

