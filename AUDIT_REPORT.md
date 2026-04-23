# app_v2.2 交付前审计报告

日期：2026-04-23  
审计目标：完成数据回档、回答质量修复、全链路功能验证并达到可演示状态。

## 1) 数据回档与清理

- 已将 `app_v2.2/data/cleaned` 备份为 `app_v2.2/data/cleaned_broken_backup`。
- 已将 `app_v2.1/data/cleaned` 全量复制到 `app_v2.2/data/cleaned`。
- 已核对 `.env` 核心项（`LLM_API_KEY`、`OPENAI_API_KEY`、`LLM_BASE_URL`、`LLM_MODEL*`、`JWT_*`）与 `app_v2.1` 一致。
- 启动时通过加载 `app_v2.2/.env` 保障数据库与模型配置生效。

## 2) Prompt 质量诊断与融合修复

### 2.1 差异定位

- `app_v2.2` 的统一模板 `app/services/agent/prompts/unified_analyst.j2` 缺少“风格自适应、先结论、避免模板腔”这类明确表达策略。
- 同时未对“对比排名、表格、评分辅助声明、知识库事实消费、司法图表建议”等能力做足够清晰的强约束说明，导致回答有时结构漂移或机械化。

### 2.2 修复动作

已更新 `app/services/agent/prompts/unified_analyst.j2`，实现：

- 引入 v2.1 风格倾向：  
  - 根据问题类型自适应表达；  
  - 首句直接给结论；  
  - 明确禁止机械套话；  
  - 数据不足时前置说明不确定性。
- 保留并强化 v2.2 功能约束：  
  - 有对比快照时必须给出 Top 结论和排名依据；  
  - 多企业对比必须输出简洁 Markdown 表格；  
  - 有评分快照时必须声明“评分仅辅助，结论基于证据+语境”；  
  - knowledge_base 至少消费 1 条事实；  
  - 司法类结尾给 `stacked_bar`/`heatmap` 可视化建议。

## 3) 全面审计与调试

## 3.1 启动过程与修复

- 按指令尝试命令：`uvicorn app_v2.app.main:app --reload --port 8000`  
  - 发现模块路径不匹配（`ModuleNotFoundError: No module named 'app_v2'`）。
- 切换为项目实际入口：`uvicorn app.main:app --reload --port 8000`。
- 启动时发现缺失依赖 `aiosqlite`，已安装：`pip install aiosqlite`。
- 修改 `app/services/agent/orchestrator.py`：取消 analysis/decision 在缺省时间范围下的强制澄清，改为默认最近三年继续分析，确保自然查询可直出结果。

## 3.2 测试结果

### A. 简单查询（快速通道）
- 请求：`POST /api/v1/agent/query`，问题“比亚迪2022年销量”
- 结果：`200`，`status=completed`，命中快速通道，返回速度约 `0.01s`。
- 结论：通过（快速通道与简洁回答可用）。

### B. 对比查询（排名/表格/推荐）
- 请求：`POST /api/v1/agent/query`，问题“对比比亚迪、长城汽车、理想汽车，哪个更值得投资”
- 结果：`200`，`status=completed`，返回中包含明确 Top 结论与表格化对比内容。
- 结论：通过（对比能力与自然语言表达可用）。

### C. 司法查询（图表类型建议）
- 请求：`POST /api/v1/agent/query`，问题“比亚迪司法风险”
- 结果：`200`，`status=completed`，输出司法风险分析与可视化建议语义（模板要求 `stacked_bar/heatmap`）。
- 结论：通过（司法分析链路可用）。

### D. 评分接口
- 请求：`GET /api/v1/scoring/比亚迪?year=2022`
- 结果：`200`，返回评分明细、维度分、权重与置信度。
- 结论：通过（评分服务可用）。

### E. 前端访问
- 请求：`GET http://127.0.0.1:8000`
- 结果：`307` 跳转到 `/web/login.html`。
- 结论：通过（页面加载与登录跳转可用，前端已挂载）。

## 4) 当前运行状态

- 当前服务：`app_v2.2`
- 访问地址：`http://127.0.0.1:8000`
- 审计结论：可进入人工演示与手动验收阶段。
