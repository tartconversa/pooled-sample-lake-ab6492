"""数据湖业务口径测试。

覆盖：持续接收幂等、材料台账不重复记账、混池份额分摊、质控失败留分母、
重复导出去重、版本可比/不可比、方法换版影响、不闭合分支不报全覆盖、历史回放。
"""
from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from sourcepack.lakehouse import impact, ingest, lake, shared
from sourcepack.lakehouse.shared import connect

ROOT = Path(__file__).resolve().parents[1]
TZ = shared.DEFAULT_TZ


def ts(text: str) -> dt.datetime:
    return shared.parse_ts(text)


class LakeCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "lake.duckdb"
        self.con = connect(self.db)
        lake.load_pack(self.con, ROOT, ts("2026-10-20T10:00:00+08:00"), "base")
        impact.snapshot(self.con, "B001")

    def tearDown(self) -> None:
        self.con.close()
        self.tmp.cleanup()

    def scalar(self, sql: str):
        return self.con.execute(sql).fetchone()[0]

    def rows(self, sql: str):
        return self.con.execute(sql).fetchall()

    # ---------- 接收 ----------
    def test_ingest_is_idempotent(self):
        before = self.scalar("SELECT count(*) FROM bronze.raw_row")
        batch = lake.load_pack(self.con, ROOT, ts("2026-10-21T10:00:00+08:00"), "repeat")
        self.assertTrue(all(v == "duplicate" for v in batch["ingest"]["files"].values()))
        self.assertEqual(before, self.scalar("SELECT count(*) FROM bronze.raw_row"))
        self.assertEqual(2, self.scalar(
            "SELECT count(DISTINCT batch_id) FROM bronze.file_receipt WHERE status='duplicate' OR status='accepted'"))

    def test_instrument_doc_retained_immutably(self):
        self.assertEqual(1, self.scalar("SELECT count(*) FROM bronze.instrument_doc"))
        # EX2COPY 作为原始导出保留在 bronze
        self.assertEqual(2, self.scalar(
            "SELECT count(DISTINCT export_id) FROM bronze.instrument_raw_measurement WHERE run_id='R2'"))

    # ---------- 材料台账与血缘 ----------
    def test_consume_irreversible_return_does_not_restore(self):
        # A1 退回冷库不恢复其被分装/废弃的耗用
        a1 = self.rows("SELECT inflow_ul, transform_consumed_ul, discarded_ul, return_events, balance_ul "
                       "FROM silver.material_balance WHERE material_id='A1'")[0]
        inflow, tcons, discarded, returns, balance = a1
        self.assertEqual(1, returns)
        self.assertEqual(balance, inflow - tcons - discarded)

    def test_ancestor_and_child_not_double_counted_in_retained(self):
        physical, root_equiv = self.rows(
            "SELECT retained_physical_ul, retained_root_equiv_ul "
            "FROM gold.sample_retained WHERE root_material_id='S1'")[0]
        # 物理体积跨祖先/子管会重复（190），但根等效体积按成分份额折算不翻倍：
        # S1本管60 + A1 15 + POOL 12.5 + DIL 4 + DIL2 0.5 = 92，
        # 等于初始100减去全部真实耗用，且不超过初始量。
        self.assertGreater(physical, root_equiv)
        self.assertAlmostEqual(92.0, root_equiv, places=6)
        self.assertLessEqual(root_equiv, 100.0)

    def test_pool_consumption_allocated_by_share_not_whole_to_each(self):
        # R2 混池孔 5uL，S1/S2 各分摊 2.5，而不是各自记 5
        charges = {r[0]: r[1] for r in self.rows(
            "SELECT root_material_id, root_charged_ul FROM gold.root_consumption WHERE run_id='R2'")}
        self.assertAlmostEqual(2.5, charges["S1"], places=6)
        self.assertAlmostEqual(2.5, charges["S2"], places=6)

    def test_buffer_contributes_volume_not_sample_component(self):
        # DIL 含缓冲液，但其根成分只有 S1/S2 各 5uL，缓冲液不出现为根
        roots = {r[0] for r in self.rows(
            "SELECT DISTINCT root_material_id FROM silver.material_origin WHERE material_id='DIL'")}
        self.assertEqual({"S1", "S2"}, roots)

    def test_dilution_factor(self):
        # T3 稀释：非缓冲原样 10uL / 总体积 100uL => 10 倍
        self.assertAlmostEqual(10.0, self.scalar(
            "SELECT dilution_factor FROM silver.transform_closure WHERE transform_id='T3'"), places=6)

    # ---------- 实验执行与证据 ----------
    def test_failed_run_keeps_workload_but_not_evidence(self):
        # 实际占孔4，有效占孔3（R3 失败仍占孔）
        self.assertEqual(4, self.scalar("SELECT sum(actual_wells) FROM gold.lab_well_workload"))
        self.assertEqual(3, self.scalar("SELECT sum(effective_wells) FROM gold.lab_well_workload"))
        # R3 失败仍计实际耗用
        self.assertGreater(self.scalar(
            "SELECT sum(consumed_in_failed_runs_ul) FROM gold.lab_consumption"), 0)
        # R3 在证据表中保留且不合格，没有从分母消失
        self.assertEqual(2, self.scalar(
            "SELECT count(*) FROM silver.well_root_evidence WHERE run_id='R3' AND eligible=false"))

    def test_duplicate_export_not_new_work(self):
        # EX2/EX2COPY 去重为一条测量事实
        self.assertEqual(1, self.scalar(
            "SELECT count(*) FROM silver.measurement_fact WHERE run_id='R2' AND channel='signal'"))
        self.assertEqual(2, self.scalar(
            "SELECT export_count FROM silver.measurement_fact WHERE run_id='R2' AND channel='signal'"))

    def test_pool_only_qualitative_no_per_sample_value(self):
        kinds = {r[0] for r in self.rows(
            "SELECT DISTINCT coverage_kind FROM silver.well_root_evidence WHERE is_pooled AND eligible")}
        self.assertEqual({"pooled_qualitative"}, kinds)

    def test_version_comparability(self):
        # v1 signal 与 v2 mass_concentration 归到同一 analyte，单位统一 ug/L
        analytes = self.rows(
            "SELECT DISTINCT analyte, canonical_unit FROM silver.normalized_measurement "
            "WHERE analyte='analyte_conc'")
        self.assertTrue(all(unit == "ug/L" for _, unit in analytes))
        # ratio 不可比，保留为 unmapped
        self.assertEqual(2, self.scalar(
            "SELECT count(*) FROM silver.normalized_measurement WHERE analyte='__unmapped__ratio'"))

    def test_coverage_attempts_include_failures(self):
        s1 = self.rows("SELECT attempt_wells, eligible_wells, has_evidence FROM gold.sample_coverage "
                       "WHERE root_material_id='S1'")[0]
        self.assertEqual((3, 2, True), s1)

    # ---------- 补交与影响 ----------
    def _write_pack(self, folder: Path, files: dict[str, str]):
        d = folder / "data"
        d.mkdir(parents=True, exist_ok=True)
        for name, content in files.items():
            (d / name).write_text(content, encoding="utf-8")

    def test_unclosed_branch_flagged_but_retained(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            pack = Path(raw_dir)
            self._write_pack(pack, {
                "transforms.csv": "transform_id,kind,at\nT5,mix,2026-10-19T08:00:00+08:00\n",
                "transform_inputs.csv": ("input_id,transform_id,material_id,volume_ul\n"
                                         "I7,T5,S3,10\nI8,T5,X9,30\n"),
                "transform_outputs.csv": "material_id,transform_id,volume_ul\nPOOL2,T5,50\n",
            })
            batch = ingest.ingest_batch(
                self.con,
                {t: pack / "data" / f"{t}.csv" for t in
                 ("transforms", "transform_inputs", "transform_outputs")},
                None, ts("2026-10-22T10:00:00+08:00"), "unclosed")
            lake.rebuild_silver_gold(self.con)
            impact.snapshot(self.con, batch["batch_id"])
            row = self.rows("SELECT volume_closed, sources_complete FROM silver.transform_closure "
                            "WHERE transform_id='T5'")[0]
            self.assertEqual((False, False), row)
            # 未知来源边仍被保留
            self.assertTrue(any(r[0] == "X9" for r in self.rows(
                "SELECT parent_material_id FROM silver.lineage_edge WHERE transform_id='T5'")))

    def test_rule_revision_changes_coverage_and_impact(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            pack = Path(raw_dir)
            self._write_pack(pack, {
                "method_rules.csv": ("method,allows_pool,max_dilution,max_freeze_thaw\n"
                                     "M1,false,5,2\nM2,true,2,3\n"),
            })
            batch = ingest.ingest_batch(
                self.con, {"method_rules": pack / "data" / "method_rules.csv"},
                None, ts("2026-10-23T10:00:00+08:00"), "tighten")
            lake.rebuild_silver_gold(self.con)
            impact.snapshot(self.con, batch["batch_id"])
            report = impact.assess_impact(self.con, batch["batch_id"])
            affected = {a["root_material_id"] for a in report["affected"]}
            self.assertEqual({"S1", "S2"}, affected)
            # 旧版本仍在 bronze
            self.assertEqual(2, self.scalar(
                "SELECT count(*) FROM bronze.raw_row WHERE table_name='method_rules' AND business_key='M2'"))
            # 缺口原因明确
            reasons = {r[0] for r in self.rows(
                "SELECT gap_reason FROM gold.coverage_gap WHERE root_material_id IN ('S1','S2')")}
            self.assertEqual({"dilution_exceeded"}, reasons)

    def test_asof_replay_restores_prior_coverage(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            pack = Path(raw_dir)
            self._write_pack(pack, {
                "method_rules.csv": ("method,allows_pool,max_dilution,max_freeze_thaw\n"
                                     "M1,false,5,2\nM2,true,2,3\n"),
            })
            batch = ingest.ingest_batch(
                self.con, {"method_rules": pack / "data" / "method_rules.csv"},
                None, ts("2026-10-23T10:00:00+08:00"), "tighten")
            lake.rebuild_silver_gold(self.con)
            # 回到收紧前的时点重算
            lake.rebuild_silver_gold(self.con, as_of=ts("2026-10-22T23:59:00+08:00"))
            has = self.scalar(
                "SELECT has_evidence FROM gold.sample_coverage WHERE root_material_id='S1'")
            self.assertTrue(has)


if __name__ == "__main__":
    unittest.main()
