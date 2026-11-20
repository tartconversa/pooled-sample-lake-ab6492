# 研发样本分装混池的实验事实仓

合成业务资料（非临床、无个人信息与诊断结论）的分层数据湖。目标是把**项目组的证据/留样口径**与**实验室的真实运行口径**分开：质控失效的板不再算作可用证据，但它实际占用的孔位和样本消耗不会被抹掉。

字段与业务规则以 `docs/资料口径.md` 为准；`data/catalog.json` 是输入资料的完整性目录。

## 三层架构

全部落在一个 DuckDB 库（默认 `lake/lake.duckdb`），分三个 schema：

| 层 | 内容 | 关键性质 |
| --- | --- | --- |
| `bronze` | 接收批次、文件回执、原始行（`raw_row`）、仪器嵌套原件（`instrument_doc`）与展开测量 | 只追加、不可变；内容哈希幂等，重复提交只记回执不增事实；同业务键修订多版本共存 |
| `silver` | 材料台账与成分血缘、转化闭合、冻融、孔位占用、去重测量、归一化测量、孔-根证据资格 | 由 bronze 最新版本可重放计算 |
| `gold` | 项目组覆盖/留样/缺口表，实验室运行/占孔/消耗表，下钻表，影响报告 | 面向双方核对的可读仓表 |

### 口径如何分开

- **证据（项目组）**：`gold.sample_coverage` 只统计质控通过且满足方法契约（混池许可、稀释倍数、冻融次数、血缘闭合）的孔；失败运行仍计入 `attempt_*` 分母。混池只产生 `pooled_qualitative` 定性覆盖，不反推单原样数值。
- **工作量与耗用（实验室）**：`gold.lab_run_workload` / `lab_well_workload` 把实际值与有效值分列，`qc=failed` 的运行保留实际占孔和耗用，另列有效列。
- **留样**：`gold.sample_retained` 同时给物理体积与根等效体积。物理体积跨祖先/子管会重复；根等效体积按成分份额折算，分装不翻倍，且不超过初始量。退回冷库（return）只改保管位置，不恢复任何耗用。

### 材料血缘规则

- 缓冲液贡献体积、不贡献原样成分；混池分取按输入体积比例携带根成分。
- 混池消耗按份额分摊到各根原样（`gold.root_consumption`），不会给每个来源记整池消耗。
- 转化在 `silver.transform_closure` 标注体积闭合与来源完整性；不闭合/来源不全的分支原始证据保留（含未知来源边），但在证据资格上标 `lineage_unclosed`，不会被报成全部覆盖。
- 祖先管与含其成分的子管不会被同时当作库存相加。

### 方法换版

`silver.channel_mapping` 显式声明可比较口径（含线性换算）。v1 `signal(ng/mL)` 与 v2 `mass_concentration(ug/L)` 归到同一 analyte（ug/L，1:1）；v2 的 `ratio` 是无量纲新指标，保留为 `__unmapped__ratio` 不可比较测量，不硬套成历史同义词。

## 使用

环境：Python 3.11.2，依赖见 `requirements.txt`（仅 duckdb）。

```bash
python3 -m pip install -r requirements.txt

# 建库并接收基础资料包
python3 -m sourcepack.lakehouse.cli init --db lake/lake.duckdb --pack . --at 2026-10-20T10:00:00+08:00

# 补交一个资料批次（新目录，含部分表即可），自动重算并输出影响
python3 -m sourcepack.lakehouse.cli ingest --db lake/lake.duckdb --pack <补交目录> --label L2

# 查看任一仓表
python3 -m sourcepack.lakehouse.cli show --db lake/lake.duckdb --table gold.sample_coverage

# 按原样下钻：覆盖 → 留样 → 转化血缘 → 逐孔资格 → 原文件测量
python3 -m sourcepack.lakehouse.cli drill --db lake/lake.duckdb --root S1

# 补交影响报告（哪些原样的覆盖/留样发生变化）
python3 -m sourcepack.lakehouse.cli impact --db lake/lake.duckdb

# 按历史接收时点回放口径
python3 -m sourcepack.lakehouse.cli replay --db lake/lake.duckdb --at 2026-10-22T23:59:00+08:00
```

不可信的外部资料应放在独立目录，以路径参数传入；脚本与资料目录分离。

## 持续接收与影响核实

- 每次 `ingest` 形成一个批次（`bronze.ingest_batch`），业务时点与接收时点分别记录。
- 相同内容哈希的文件判为 `duplicate`，不产生新事实。
- 每次重算后对覆盖结果打快照（`gold.coverage_snapshot`），补交前后对比生成 `gold.impact_report`，列出受影响根原样及变化（如 `evidence_lost`、有效孔增减、留样变化）。

## 测试

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q sourcepack tests
```

`tests/test_lakehouse.py` 固化关键口径：接收幂等、消耗不可逆、混池份额分摊、失败留分母、重复导出去重、版本可比/不可比、不闭合分支不报全覆盖、方法换版影响与历史回放。原有的 `sourcepack.catalog.inspect` 资料自检仍可用；摘要比对对 CRLF/LF 行尾差异做双向归一化，换行变化不被误判为内容改动。
