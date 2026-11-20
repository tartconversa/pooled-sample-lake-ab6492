"""研发样本实验数据湖：分层建模与仓表。

分层：
- bronze  持续接收入湖，原始文件与记录不可变、可回溯
- silver  多粒度事实整理：材料台账、实验执行、证据资格
- gold    可读仓表：项目组覆盖/留样/缺口，实验室工作量/耗用

本包只解释合成研发资料，不产生临床结论。
"""

__all__ = ["shared", "ingest", "lineage", "materials", "experiment", "marts", "lake", "impact"]
