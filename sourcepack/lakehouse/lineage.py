"""材料血缘与台账（silver）。

在 bronze 最新版本事实上计算：
- material_origin      每个材料按根原样的成分份额（混池分取按输入体积比例携带）
- lineage_edge         转化的有向边（来源 -> 产物），带体积与占比
- root_path            根原样到每个材料的路径，及路径上的稀释/冻融
- material_balance     每材料物理台账：收入/耗用/废弃/退回（不还原耗用）/余额
- transform_closure    每次转化的体积闭合与来源完整性
- freeze_thaw_state    每个材料、每个根原样视角的累计冻融次数

规则见 docs/资料口径.md：缓冲液贡献体积不贡献原样成分；consume 不可逆；
退回只变保管位置；混池消耗按份额分摊，不能给每个来源记整池消耗。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

import duckdb

from . import shared


@dataclass
class Material:
    material_id: str
    kind: str
    project: str | None
    initial_volume: float | None
    # 根原样 -> 成分体积（uL）；缓冲液与来源不全部分不进入
    composition: dict[str, float] = field(default_factory=dict)
    # 该材料创建（入流）时固定下来的根成分体积，不随后续耗用改变
    origin_components: dict[str, float] = field(default_factory=dict)
    inflow: float = 0.0
    transform_consumed: float = 0.0
    well_consumed: float = 0.0
    discarded: float = 0.0
    returns: int = 0
    # 该材料自身（作为根时从零计）发生的冻融次数
    own_freeze_thaw: int = 0
    exists: bool = True


def build_lineage(con: duckdb.DuckDBPyConnection, as_of=None) -> dict:
    rows = shared_ingest_latest(con, as_of)
    materials = {
        r["material_id"]: Material(
            r["material_id"], r["kind"], r["project"],
            float(r["volume_ul"]) if r["volume_ul"] is not None else None,
        )
        for r in rows["materials"]
    }
    # 根材料：sample 以自身为根，成分体积等于实收体积
    for m in materials.values():
        if m.kind == "sample" and m.initial_volume:
            m.composition[m.material_id] = m.initial_volume
            m.origin_components = dict(m.composition)
            m.inflow = m.initial_volume

    transforms = {r["transform_id"]: r for r in rows["transforms"]}
    inputs_by_t: dict[str, list[dict]] = defaultdict(list)
    for r in rows["transform_inputs"]:
        inputs_by_t[r["transform_id"]].append(r)
    outputs_by_t: dict[str, list[dict]] = defaultdict(list)
    for r in rows["transform_outputs"]:
        outputs_by_t[r["transform_id"]].append(r)

    edges: list[dict] = []
    origins_rows: list[dict] = []
    closures: list[dict] = []
    warnings: list[str] = []

    # 转化按业务时点排序（CSV 顺序不代表事件顺序）
    ordered = sorted(transforms.values(), key=lambda t: shared.parse_ts(t["at"]))  # 原始字段名
    produced_by = {o["material_id"]: o["transform_id"]
                   for outs in outputs_by_t.values() for o in outs}

    for t in ordered:
        tid = t["transform_id"]
        ins = inputs_by_t.get(tid, [])
        outs = outputs_by_t.get(tid, [])
        in_total = sum(float(i["volume_ul"]) for i in ins)
        out_total = sum(float(o["volume_ul"]) for o in outs)
        unknown_sources = [i["input_id"] for i in ins if i["material_id"] not in materials]
        # 余额充足性校验（在本次消耗前）；缓冲液按实收库存单独口径，不参与转化余额推断
        for i in ins:
            mat = materials.get(i["material_id"])
            if mat is None or mat.kind == "buffer":
                continue
            need = float(i["volume_ul"])
            if mat.initial_volume is not None and mat.inflow - mat.transform_consumed \
                    - mat.well_consumed - mat.discarded < need - 1e-9:
                warnings.append(f"{tid}: 来源 {mat.material_id} 余额不足 {need}")
            mat.transform_consumed += need

        # 来源成分合并：按输入体积比例携带。来源未知的贡献无法归属。
        blended: dict[str, float] = defaultdict(float)
        for i in ins:
            mat = materials.get(i["material_id"])
            if mat is None:
                continue
            for root, vol in mat.composition.items():
                share = vol * (float(i["volume_ul"]) / mat._volume()) if mat._volume() else 0.0
                blended[root] += share
        in_sample_volume = sum(blended.values())
        dilution = (in_total / in_sample_volume) if in_sample_volume > 0 else None

        closed = abs(in_total - out_total) <= 1e-9
        complete = not unknown_sources
        closures.append({
            "transform_id": tid, "kind": t["kind"], "occurred_at": t["at"],
            "input_volume_ul": in_total, "output_volume_ul": out_total,
            "volume_closed": closed, "sources_complete": complete,
            "unknown_input_ids": ",".join(unknown_sources),
            "dilution_factor": dilution,
        })
        if not closed:
            warnings.append(f"{tid}: 体积不闭合 入{in_total} 出{out_total}")
        if not complete:
            warnings.append(f"{tid}: 来源不全 {unknown_sources}")

        for o in outs:
            child_id = o["transform_id"] and o["material_id"]
            out_vol = float(o["volume_ul"])
            if child_id not in materials:
                # 产物未在 materials 登记也要建节点（真实产管）
                materials[child_id] = Material(child_id, "derived", None, None)
            child = materials[child_id]
            child.inflow += out_vol
            factor = (out_vol / in_total) if in_total > 0 else 0.0
            for i in ins:
                parent = materials.get(i["material_id"])
                parent_vol = parent._volume() if parent else None
                edge = {
                    "transform_id": tid, "transform_kind": t["kind"], "occurred_at": t["at"],
                    "parent_material_id": i["material_id"],
                    "child_material_id": child_id,
                    "input_volume_ul": float(i["volume_ul"]),
                    "output_volume_ul": out_vol,
                    "parent_known": parent is not None,
                    "transfer_share": (float(i["volume_ul"]) / parent_vol) if parent_vol else None,
                    "output_share": (float(i["volume_ul"]) / in_total) if in_total else None,
                }
                edges.append(edge)
            for root, comp_vol in blended.items():
                child.composition[root] = child.composition.get(root, 0.0) + comp_vol * factor
            # 固化创建时的根成分（历史事实，不随后续耗用/再转化改变）
            child.origin_components = dict(child.composition)

    # 材料经历事件
    for ev in rows["material_events"]:
        mat = materials.get(ev["material_id"])
        if mat is None:
            warnings.append(f"事件 {ev['event_id']} 引用未知材料 {ev['material_id']}")
            continue
        if ev["kind"] == "freeze_thaw":
            mat.own_freeze_thaw += 1
        elif ev["kind"] == "return":
            mat.returns += 1  # 只变保管位置，不还原任何耗用
        elif ev["kind"] == "discard":
            mat.discarded += float(ev["volume_ul"] or 0.0)
            if mat.initial_volume is not None and mat.balance() < -1e-9:
                warnings.append(f"{mat.material_id}: 废弃后余额为负")

    # 孔位真实消耗（失败运行也消耗；在 build_experiment 之外也独立登记）
    for w in rows["well_inputs"]:
        mat = materials.get(w["material_id"])
        if mat is None:
            warnings.append(f"加样 {w['input_id']} 引用未知材料 {w['material_id']}")
            continue
        mat.well_consumed += float(w["volume_ul"])
        if mat.initial_volume is not None and mat.balance() < -1e-9:
            warnings.append(f"{mat.material_id}: 实验消耗后余额为负")

    # 根路径与冻融继承
    root_paths = _build_root_paths(materials, edges, rows)

    # 持久化
    _persist(con, materials, edges, closures, root_paths, rows, produced_by)
    return {
        "materials": len(materials),
        "edges": len(edges),
        "closures": closures,
        "warnings": warnings,
    }


def _build_root_paths(materials: dict[str, Material], edges: list[dict], rows) -> list[dict]:
    """每个材料对每个根原样的路径、累计稀释、累计冻融。"""
    children: dict[str, list[dict]] = defaultdict(list)
    for e in edges:
        children[e["parent_material_id"]].append(e)
    paths: list[dict] = []

    def walk(node_id: str, root: str, dilution: float, ft: int, trail: list[str]):
        mat = materials.get(node_id)
        own_ft = mat.own_freeze_thaw if mat else 0
        cur_ft = ft + own_ft
        paths.append({
            "root_material_id": root, "material_id": node_id,
            "path": ">".join(trail),
            "cum_dilution_factor": dilution,
            "freeze_thaw_count": cur_ft,
        })
        for e in children.get(node_id, []):
            edge_dil = _edge_dilution(e, materials)
            walk(e["child_material_id"], root, dilution * edge_dil, cur_ft,
                 trail + [e["child_material_id"]])

    for root_id, m in materials.items():
        if m.kind == "sample":
            walk(root_id, root_id, 1.0, 0, [root_id])
    return paths


def _edge_dilution(e: dict, materials: dict[str, Material]) -> float:
    """单条边携带的稀释倍数：输出份额 / 输入中该来源占比（缓冲液稀释体现于此）。"""
    if not e["parent_known"] or not e["transfer_share"]:
        return 1.0
    return (e["output_share"] or 0.0) / e["transfer_share"] if e["transfer_share"] else 1.0


def _persist(con, materials, edges, closures, root_paths, rows, produced_by) -> None:
    con.execute("DROP TABLE IF EXISTS silver.material")
    con.execute("""
        CREATE TABLE silver.material (
            material_id VARCHAR PRIMARY KEY, kind VARCHAR, project VARCHAR,
            initial_volume_ul DOUBLE, is_root_sample BOOLEAN, producing_transform_id VARCHAR
        )""")
    con.execute("DROP TABLE IF EXISTS silver.material_origin")
    con.execute("""
        CREATE TABLE silver.material_origin (
            material_id VARCHAR, root_material_id VARCHAR, component_volume_ul DOUBLE,
            PRIMARY KEY (material_id, root_material_id)
        )""")
    con.execute("DROP TABLE IF EXISTS silver.lineage_edge")
    con.execute("""
        CREATE TABLE silver.lineage_edge (
            transform_id VARCHAR, transform_kind VARCHAR, occurred_at TIMESTAMP,
            parent_material_id VARCHAR, child_material_id VARCHAR,
            input_volume_ul DOUBLE, output_volume_ul DOUBLE,
            parent_known BOOLEAN, transfer_share DOUBLE, output_share DOUBLE
        )""")
    con.execute("DROP TABLE IF EXISTS silver.root_path")
    con.execute("""
        CREATE TABLE silver.root_path (
            root_material_id VARCHAR, material_id VARCHAR, path VARCHAR,
            cum_dilution_factor DOUBLE, freeze_thaw_count INTEGER
        )""")
    con.execute("DROP TABLE IF EXISTS silver.material_balance")
    con.execute("""
        CREATE TABLE silver.material_balance (
            material_id VARCHAR PRIMARY KEY, kind VARCHAR, project VARCHAR,
            inflow_ul DOUBLE, transform_consumed_ul DOUBLE, well_consumed_ul DOUBLE,
            discarded_ul DOUBLE, return_events INTEGER, balance_ul DOUBLE,
            negative_balance BOOLEAN
        )""")
    con.execute("DROP TABLE IF EXISTS silver.transform_closure")
    con.execute("""
        CREATE TABLE silver.transform_closure (
            transform_id VARCHAR PRIMARY KEY, kind VARCHAR, occurred_at TIMESTAMP,
            input_volume_ul DOUBLE, output_volume_ul DOUBLE,
            volume_closed BOOLEAN, sources_complete BOOLEAN,
            unknown_input_ids VARCHAR, dilution_factor DOUBLE
        )""")
    con.execute("DROP TABLE IF EXISTS silver.material_event")
    con.execute("""
        CREATE TABLE silver.material_event (
            event_id VARCHAR PRIMARY KEY, material_id VARCHAR, kind VARCHAR,
            occurred_at TIMESTAMP, volume_ul DOUBLE
        )""")
    for mid, m in materials.items():
        con.execute("INSERT INTO silver.material VALUES (?, ?, ?, ?, ?, ?)", [
            mid, m.kind, m.project, m.initial_volume, m.kind == "sample",
            produced_by.get(mid),
        ])
        for root, vol in m.origin_components.items():
            con.execute("INSERT INTO silver.material_origin VALUES (?, ?, ?)",
                        [mid, root, vol])
        bal = m.balance()
        con.execute("INSERT INTO silver.material_balance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            mid, m.kind, m.project, m.inflow, m.transform_consumed, m.well_consumed,
            m.discarded, m.returns, bal,
            bal < -1e-9,
        ])
    for e in edges:
        con.execute("INSERT INTO silver.lineage_edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            e["transform_id"], e["transform_kind"], shared.parse_ts(e["occurred_at"]),
            e["parent_material_id"], e["child_material_id"], e["input_volume_ul"],
            e["output_volume_ul"], e["parent_known"], e["transfer_share"], e["output_share"],
        ])
    for p in root_paths:
        con.execute("INSERT INTO silver.root_path VALUES (?, ?, ?, ?, ?)", [
            p["root_material_id"], p["material_id"], p["path"],
            p["cum_dilution_factor"], p["freeze_thaw_count"],
        ])
    for c in closures:
        con.execute("INSERT INTO silver.transform_closure VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            c["transform_id"], c["kind"], shared.parse_ts(c["occurred_at"]),
            c["input_volume_ul"], c["output_volume_ul"], c["volume_closed"],
            c["sources_complete"], c["unknown_input_ids"], c["dilution_factor"],
        ])
    for ev in rows["material_events"]:
        con.execute("INSERT INTO silver.material_event VALUES (?, ?, ?, ?, ?)", [
            ev["event_id"], ev["material_id"], ev["kind"], shared.parse_ts(ev["at"]),  # 原始字段
            float(ev["volume_ul"]) if ev["volume_ul"] is not None else None,
        ])


def shared_ingest_latest(con, as_of):
    """延迟导入避免循环依赖。"""
    from .ingest import latest_rows
    return latest_rows(con, as_of)


# 给数据类补两个方法（放类外会打断 dataclass 字段顺序，这里挂到原型上）
def _volume(self: Material) -> float:
    """该材料当前可用的物理体积口径（收入减全部出库），用于比例计算基准。"""
    if self.initial_volume is None:
        base = self.inflow
    else:
        base = self.inflow
    return base


def _balance(self: Material) -> float:
    """当前物理余额：全部入流（初始实收或转化产出）减全部出库。

    退回（return）只改变保管位置，不还原任何耗用，因此不进入本式。
    """
    return self.inflow - self.transform_consumed - self.well_consumed - self.discarded


Material._volume = _volume  # type: ignore[attr-defined]
Material.balance = _balance  # type: ignore[attr-defined]
