import math
import unittest

import pandas as pd

from data_processing_scripts.create_interactive_damage_signature_viewer import (
    CONCORDANCE_STYLE,
    CRITICAL_OVERLAP_COLORS,
    HTML_TEMPLATE,
    KOSIER_2026_SELC_CRITICAL_J_CM2,
    _v2_key_tail,
    _v2_target_label,
    concordance_3d_plot_payload,
    dex_series,
    energy_balance_plot_payload,
    energy_delta_plot_payload,
    irradiation_energy_summary,
    v2_interval_overlap_plot_payload,
    v2_overlap_summary_plot_payload,
    v2_severity_parity_plot_payload,
)
from data_processing_scripts.export_proxy_candidate_energy_v2_csv import (
    _flatten_array,
)
from data_processing_scripts.export_proxy_truth_curation_queue import (
    _flatten_array as _flatten_queue_array,
)


def _irradiation_record(deposited_j: float) -> dict:
    """One DUT-A irradiation event with fixed area and a varying deposited energy."""
    return {
        "source": "irradiation",
        "device_label": "DUT-A",
        "event_type": "SELCI",
        "radiation_deposited_energy_j": deposited_j,
        "radiation_deposited_energy_total_j": deposited_j * 2.0,
        "electrical_terminal_energy_j": 0.5,
        # volume / thickness gives a fixed 0.02 cm2 active area per record.
        "energy_density_active_volume_cm3": 2.0e-5,
        "se_depletion_active_thickness_um": 10.0,
        "se_depletion_stored_energy_j_cm2": 30.0e-6,
    }


class InteractiveDamageSignatureViewerTests(unittest.TestCase):
    def _records(self) -> pd.DataFrame:
        # Five DUT-A events (>= MIN_DEVICE_RECORDS) plus one avalanche row that
        # must be excluded from the irradiation-only per-device summary.
        deposited = [1.0e-6, 2.0e-6, 3.0e-6, 4.0e-6, 10.0e-6]
        rows = [_irradiation_record(value) for value in deposited]
        rows.append(
            {
                "source": "avalanche",
                "device_label": "DUT-A",
                "radiation_deposited_energy_j": 100.0,
                "radiation_deposited_energy_total_j": 100.0,
                "electrical_terminal_energy_j": 100.0,
                "energy_density_active_volume_cm3": 100.0,
                "se_depletion_active_thickness_um": 10.0,
                "se_depletion_stored_energy_j_cm2": 100.0,
            }
        )
        return pd.DataFrame(rows)

    def test_summary_reports_per_device_mean_and_median(self):
        summary = irradiation_energy_summary(self._records())

        self.assertEqual(len(summary["devices"]), 1)
        device = summary["devices"][0]
        self.assertEqual(device["name"], "DUT-A")
        self.assertEqual(device["n_records"], 5)
        self.assertAlmostEqual(device["active_area_median_cm2"], 0.02)

        metrics = {m["label"]: m for m in device["metrics"]}
        # mean of [1,2,3,4,10] uJ = 4 uJ; median = 3 uJ.
        self.assertAlmostEqual(metrics["Ionizing deposited"]["mean_j"], 4.0e-6)
        self.assertAlmostEqual(metrics["Ionizing deposited"]["median_j"], 3.0e-6)
        self.assertEqual(metrics["Ionizing deposited"]["recorded_count"], 5)
        # Terminal energy is constant, so mean and median coincide.
        self.assertAlmostEqual(metrics["Terminal electrical"]["mean_j"], 0.5)
        self.assertAlmostEqual(metrics["Terminal electrical"]["median_j"], 0.5)
        # Kosier SELC needed is the areal threshold times the fixed 0.02 cm2 area.
        self.assertAlmostEqual(
            metrics["Kosier SELC needed"]["mean_j"],
            KOSIER_2026_SELC_CRITICAL_J_CM2 * 0.02,
        )

    def test_payload_has_mean_and_median_traces_and_device_filter(self):
        payload = energy_balance_plot_payload(self._records())

        self.assertEqual(len(payload["traces"]), 2)
        mean_trace, median_trace = payload["traces"]
        self.assertEqual(mean_trace["name"], "Mean")
        self.assertEqual(median_trace["name"], "Median")
        self.assertEqual(mean_trace["type"], "bar")

        mean_by_label = dict(zip(mean_trace["x"], mean_trace["y"]))
        median_by_label = dict(zip(median_trace["x"], median_trace["y"]))
        self.assertAlmostEqual(mean_by_label["Ionizing deposited"], 4.0e-6)
        self.assertAlmostEqual(median_by_label["Ionizing deposited"], 3.0e-6)
        self.assertAlmostEqual(
            mean_by_label["Kosier SELC needed"],
            KOSIER_2026_SELC_CRITICAL_J_CM2 * 0.02,
        )

        # The per-tab Plotly dropdown was replaced by the shared global device
        # filter, so the payload exposes a filter contract instead of
        # layout updatemenus.
        self.assertNotIn("updatemenus", payload["layout"])
        device_filter = payload["filter"]
        self.assertEqual(device_filter["devices"], ["DUT-A"])
        self.assertEqual(device_filter["traceDevices"], ["DUT-A", "DUT-A"])
        # On "All devices" the per-device view defaults to the only device.
        self.assertEqual(device_filter["allShowsOnly"], "DUT-A")
        self.assertIn("DUT-A", device_filter["titles"])
        self.assertIn("per-event mean and median", payload["note"])

    def test_no_qualifying_device_returns_empty_payload(self):
        # Only two irradiation records: below MIN_DEVICE_RECORDS, so no device view.
        records = pd.DataFrame([_irradiation_record(1.0e-6) for _ in range(2)])
        payload = energy_balance_plot_payload(records)
        self.assertEqual(payload["traces"], [])
        self.assertIn("No per-device irradiation energy", payload["note"])


def _v2_row(device: str, rank: int, overlap: str, key: str) -> dict:
    """One v2 candidate row with coherent (low <= point <= high) bands."""
    return {
        "mechanistic_energy_candidate_rank": rank,
        "candidate_rank_v1": 2,
        "device_type": device,
        "target_stress_record_key": key,
        "target_event_type": "SEB",
        "target_mechanistic_regime": "heavy_ion_hard_collapse_seb",
        "candidate_source": "avalanche",
        "candidate_mechanistic_regime": "avalanche_hard_collapse",
        "regime_match_class": "first_order_analog",
        "mechanistic_energy_candidate_status": "mechanistic_measured_candidate",
        "critical_severity_overlap_class": overlap,
        "localization_mismatch_log10": 3.2,
        "target_severity_low": 0.5,
        "target_severity_high": 2.0,
        "target_severity_point_ratio": 1.0,
        "candidate_severity_low": 0.4,
        "candidate_severity_high": 1.6,
        "candidate_severity_point_ratio": 0.8,
        "energy_v2_blockers": "regime_mismatch; cross_device_screening_only",
        "energy_v2_notes": "critical_severity_is_screening_descriptor_only",
        "proxy_claim_status": "curation_candidate",
        "proxy_claim_basis": "same_device_needs_truth_curation",
        "proxy_claim_blockers": "no_curated_truth_label",
        "proxy_claim_summary": "Promising row for human truth-label curation.",
        "truth_validation_status": "no_curated_truth",
        "truth_label": "",
        "truth_label_basis": "",
        "proxy_claim_status_v1": "curation_candidate",
        "decision_safe_rank_v1": 1,
        "signature_claim_quality_v1": "two_axis",
    }


class V2IntervalOverlapPayloadTests(unittest.TestCase):
    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame([
            _v2_row("DUT-A", 1, "strong_overlap", "k-a1"),
            _v2_row("DUT-A", 1, "far_miss", "k-a2"),
            _v2_row("DUT-A", 2, "partial_overlap", "k-a3"),  # rank 2 -> excluded
            _v2_row("DUT-B", 1, "partial_overlap", "k-b1"),
        ])

    def test_empty_frame_returns_empty_payload(self):
        payload = v2_interval_overlap_plot_payload(pd.DataFrame())
        self.assertEqual(payload["traces"], [])
        self.assertIn("No v2 rank-1 candidate", payload["note"])

    def test_two_traces_per_device_and_filter_contract(self):
        payload = v2_interval_overlap_plot_payload(self._frame())
        # 2 devices x (target + candidate) = 4 traces.
        self.assertEqual(len(payload["traces"]), 4)
        f = payload["filter"]
        self.assertEqual(f["devices"], ["DUT-A", "DUT-B"])
        self.assertEqual(f["traceDevices"], ["DUT-A", "DUT-A", "DUT-B", "DUT-B"])
        self.assertEqual(f["allShowsOnly"], "DUT-A")

    def test_rank2_excluded_from_first_device(self):
        payload = v2_interval_overlap_plot_payload(self._frame())
        target_trace = payload["traces"][0]  # DUT-A target trace
        # Only the two rank-1 DUT-A targets, not the rank-2 row.
        self.assertEqual(len(target_trace["y"]), 2)

    def test_candidate_marker_colors_follow_overlap_class(self):
        payload = v2_interval_overlap_plot_payload(self._frame())
        candidate_trace = payload["traces"][1]  # DUT-A candidate trace
        self.assertEqual(
            candidate_trace["marker"]["color"],
            [CRITICAL_OVERLAP_COLORS["strong_overlap"],
             CRITICAL_OVERLAP_COLORS["far_miss"]],
        )

    def test_hover_carries_status_and_blockers(self):
        payload = v2_interval_overlap_plot_payload(self._frame())
        candidate_trace = payload["traces"][1]
        # customdata[6] = v2 candidate status, customdata[9] = energy blockers,
        # customdata[11] = fail-closed proxy claim status.
        first = candidate_trace["customdata"][0]
        self.assertEqual(first[6], "mechanistic_measured_candidate")
        self.assertIn("regime_mismatch", first[9])
        self.assertEqual(first[11], "curation_candidate")
        self.assertEqual(first[13], "no_curated_truth")
        self.assertIn("Blockers", candidate_trace["hovertemplate"])
        self.assertIn("Status", candidate_trace["hovertemplate"])
        self.assertIn("Proxy claim", candidate_trace["hovertemplate"])

    def test_point_within_band_error_bars_nonnegative(self):
        payload = v2_interval_overlap_plot_payload(self._frame())
        for trace in payload["traces"]:
            err = trace["error_x"]
            self.assertTrue(all(v >= 0 for v in err["array"]))
            self.assertTrue(all(v >= 0 for v in err["arrayminus"]))


class ExporterHelperTests(unittest.TestCase):
    def test_flatten_array_join_and_null(self):
        self.assertEqual(_flatten_array(["a", "b"]), "a; b")
        self.assertEqual(_flatten_array(None), "")
        self.assertEqual(_flatten_array("already"), "already")
        self.assertEqual(_flatten_queue_array(("x", "y")), "x; y")


class V2SeverityParityPayloadTests(unittest.TestCase):
    def _frame(self) -> pd.DataFrame:
        rows = [
            _v2_row("DUT-A", 1, "strong_overlap", "k-a1"),
            _v2_row("DUT-A", 1, "far_miss", "k-a2"),
            _v2_row("DUT-A", 2, "partial_overlap", "k-a3"),  # rank 2 -> excluded
            _v2_row("DUT-B", 1, "near_miss", "k-b1"),
        ]
        # DUT-B's candidate sits decades above DUT-A's so the two devices get
        # distinct per-device axis windows.
        rows[3]["candidate_severity_point_ratio"] = 250.0
        bad = _v2_row("DUT-B", 1, "strong_overlap", "k-b2")
        bad["candidate_severity_point_ratio"] = 0.0  # non-positive -> excluded
        rows.append(bad)
        return pd.DataFrame(rows)

    def test_empty_returns_empty_payload(self):
        payload = v2_severity_parity_plot_payload(pd.DataFrame())
        self.assertEqual(payload["traces"], [])
        self.assertIn("No v2 rank-1", payload["note"])

    def test_device_traces_plus_legend_proxies_and_filter(self):
        payload = v2_severity_parity_plot_payload(self._frame())
        # 2 device traces + 5 legend-proxy traces.
        self.assertEqual(len(payload["traces"]), 2 + len(CRITICAL_OVERLAP_COLORS))
        f = payload["filter"]
        self.assertEqual(f["devices"], ["DUT-A", "DUT-B"])
        # device traces carry their device, legend proxies carry None.
        self.assertEqual(f["traceDevices"][:2], ["DUT-A", "DUT-B"])
        self.assertTrue(all(d is None for d in f["traceDevices"][2:]))
        # "All devices" genuinely shows every device on this tab.
        self.assertIsNone(f["allShowsOnly"])

    def test_per_device_axis_ranges_and_global_default(self):
        payload = v2_severity_parity_plot_payload(self._frame())
        f = payload["filter"]
        # DUT-A ratios span 0.8..1.0 -> decade window [-1, 0];
        # DUT-B spans 1.0..250 -> [0, 3]; global covers both.
        self.assertEqual(f["ranges"]["DUT-A"], [-1.0, 0.0])
        self.assertEqual(f["ranges"]["DUT-B"], [0.0, 3.0])
        self.assertEqual(f["rangeAll"], [-1.0, 3.0])
        # The default (all-devices) layout uses the global range on both axes.
        self.assertEqual(payload["layout"]["xaxis"]["range"], [-1.0, 3.0])
        self.assertEqual(payload["layout"]["yaxis"]["range"], [-1.0, 3.0])

    def test_single_point_device_range_widens_to_one_decade(self):
        rows = [_v2_row("DUT-A", 1, "strong_overlap", "k")]
        rows[0]["target_severity_point_ratio"] = 1.0
        rows[0]["candidate_severity_point_ratio"] = 1.0
        payload = v2_severity_parity_plot_payload(pd.DataFrame(rows))
        self.assertEqual(payload["filter"]["ranges"]["DUT-A"], [0.0, 1.0])

    def test_log_axes_and_guide_shapes(self):
        payload = v2_severity_parity_plot_payload(self._frame())
        layout = payload["layout"]
        self.assertEqual(layout["xaxis"]["type"], "log")
        self.assertEqual(layout["yaxis"]["type"], "log")
        # identity + 4 dex bands + 2 crosshairs.
        self.assertGreaterEqual(len(layout["shapes"]), 7)

    def test_marker_colors_follow_overlap_class_and_exclusions(self):
        payload = v2_severity_parity_plot_payload(self._frame())
        dut_a = payload["traces"][0]
        # rank-2 and non-positive rows excluded -> 2 points for DUT-A.
        self.assertEqual(len(dut_a["x"]), 2)
        self.assertEqual(
            dut_a["marker"]["color"],
            [CRITICAL_OVERLAP_COLORS["strong_overlap"],
             CRITICAL_OVERLAP_COLORS["far_miss"]],
        )
        self.assertIn("Blockers", dut_a["hovertemplate"])
        self.assertIn("Proxy claim", dut_a["hovertemplate"])

    def test_all_nonpositive_fails_to_empty(self):
        rows = [_v2_row("DUT-A", 1, "strong_overlap", "k")]
        rows[0]["target_severity_point_ratio"] = -1.0
        payload = v2_severity_parity_plot_payload(pd.DataFrame(rows))
        self.assertEqual(payload["traces"], [])


def _v2_summary_row(crit: str, term: str, power: str) -> dict:
    return {
        "mechanistic_energy_candidate_rank": 1,
        "critical_severity_overlap_class": crit,
        "terminal_energy_overlap_class": term,
        "power_rate_overlap_class": power,
        "cumulative_exposure_overlap_class": "cumulative_present",
    }


class V2OverlapSummaryPayloadTests(unittest.TestCase):
    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame([
            _v2_summary_row("far_miss", "strong_overlap", "near_miss"),
            _v2_summary_row("far_miss", "strong_overlap", "near_miss"),
            _v2_summary_row("strong_overlap", "missing_interval", "far_miss"),
        ])

    def test_empty_returns_empty_payload(self):
        payload = v2_overlap_summary_plot_payload(pd.DataFrame())
        self.assertEqual(payload["traces"], [])

    def test_stacked_bars_over_three_axes_with_counts(self):
        payload = v2_overlap_summary_plot_payload(self._frame())
        self.assertEqual(payload["layout"]["barmode"], "stack")
        by_class = {t["name"]: t for t in payload["traces"]}
        # every trace spans the 3 comparable axes.
        for t in payload["traces"]:
            self.assertEqual(t["type"], "bar")
            self.assertEqual(len(t["x"]), 3)  # critical, terminal, power
        # strong: critical=1, terminal=2, power=0
        self.assertEqual(by_class["strong overlap (equivalent)"]["x"], [1, 2, 0])
        # far_miss: critical=2, terminal=0, power=1
        self.assertEqual(by_class["far miss"]["x"], [2, 0, 1])

    def test_note_summarizes_cumulative_axis(self):
        payload = v2_overlap_summary_plot_payload(self._frame())
        self.assertIn("Cumulative", payload["note"])
        self.assertIn("cumulative_present", payload["note"])


def _conc_row(target, candidate, v1_rank, v2_rank, csr, ds, led,
              device="DUT-A", tsr=1.0) -> dict:
    return {
        "target_stress_record_key": target,
        "candidate_stress_record_key": candidate,
        "device_type": device,
        "target_event_type": "SEB",
        "candidate_source": "avalanche",
        "match_scope": "same_device",
        "v1_rank": v1_rank,
        "v2_rank": v2_rank,
        "mechanistic_energy_candidate_status": "mechanistic_measured_candidate",
        "critical_severity_overlap_class": "strong_overlap",
        "target_severity_point_ratio": tsr,
        "candidate_severity_point_ratio": csr,
        "log_energy_delta": led,
        "damage_signature_distance": ds,
        "energy_v2_blockers": "",
        "proxy_claim_status": "curation_candidate",
        "proxy_claim_basis": "same_device_needs_truth_curation",
        "truth_validation_status": "no_curated_truth",
        "proxy_claim_status_v1": "curation_candidate",
        "signature_claim_quality_v1": "two_axis",
    }


class Concordance3DPayloadTests(unittest.TestCase):
    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame([
            # T1 consensus: the v2 rank-1 pair is also v1 rank-1.
            _conc_row("T1", "c1", v1_rank=1, v2_rank=1, csr=1.0, ds=0.2, led=0.1),
            # T2 mild: v2 rank-1 (v1 rank 4) and v1 rank-1 (v2 rank 2) differ.
            _conc_row("T2", "c2a", v1_rank=4, v2_rank=1, csr=100.0, ds=0.9, led=0.5),
            _conc_row("T2", "c2b", v1_rank=1, v2_rank=2, csr=2.0, ds=0.1, led=0.2),
            # T3 strong: no v1 rank-1 in the pool (v1's pick demoted out of top-10).
            _conc_row("T3", "c3", v1_rank=6, v2_rank=1, csr=1000.0, ds=1.5, led=2.0),
        ])

    def test_empty_returns_empty_payload(self):
        payload = concordance_3d_plot_payload(pd.DataFrame())
        self.assertEqual(payload["traces"], [])
        self.assertIn("No concordance rows", payload["note"])

    def test_categories_counted_in_note(self):
        payload = concordance_3d_plot_payload(self._frame())
        self.assertIn("1 consensus", payload["note"])
        self.assertIn("1 mild disagreement", payload["note"])
        self.assertIn("1 strong disagreement", payload["note"])

    def test_marker_points_colors_and_symbols(self):
        payload = concordance_3d_plot_payload(self._frame())
        markers = next(t for t in payload["traces"]
                       if t["type"] == "scatter3d" and t["mode"] == "markers"
                       and t["x"] and t["x"][0] is not None)
        # consensus, mild v2-pick, mild v1-pick, strong = 4 plotted points.
        self.assertEqual(len(markers["x"]), 4)
        self.assertEqual(markers["marker"]["color"], [
            CONCORDANCE_STYLE["consensus"][0],
            CONCORDANCE_STYLE["v2_pick"][0],
            CONCORDANCE_STYLE["v1_pick"][0],
            CONCORDANCE_STYLE["strong_disagree"][0],
        ])
        self.assertEqual(markers["marker"]["symbol"][0], "diamond")

    def test_severity_axis_is_log_distance(self):
        payload = concordance_3d_plot_payload(self._frame())
        markers = next(t for t in payload["traces"]
                       if t["type"] == "scatter3d" and t["mode"] == "markers"
                       and t["x"] and t["x"][0] is not None)
        # y for the consensus point: |log10(1) - log10(1)| = 0; strong: log10(1000)=3.
        self.assertAlmostEqual(markers["y"][0], 0.0)
        self.assertAlmostEqual(markers["y"][3], 3.0)

    def test_hover_carries_proxy_claim_status(self):
        payload = concordance_3d_plot_payload(self._frame())
        markers = next(t for t in payload["traces"]
                       if t["type"] == "scatter3d" and t["mode"] == "markers"
                       and t["x"] and t["x"][0] is not None)
        self.assertEqual(markers["customdata"][0][8], "curation_candidate")
        self.assertEqual(markers["customdata"][0][10], "no_curated_truth")
        self.assertIn("proxy claim", markers["hovertemplate"])

    def test_connector_drawn_for_mild_disagreement(self):
        payload = concordance_3d_plot_payload(self._frame())
        conn = next(t for t in payload["traces"]
                    if t["type"] == "scatter3d" and t["mode"] == "lines"
                    and t["x"] and t["x"][0] is not None)
        # segment v1-pick -> v2-pick -> None (damage-sig x: 0.1 then 0.9).
        self.assertEqual(conn["x"][:3], [0.1, 0.9, None])

    def test_filter_contract_and_legend_proxies(self):
        payload = concordance_3d_plot_payload(self._frame())
        f = payload["filter"]
        self.assertEqual(f["devices"], ["DUT-A"])
        # "All devices" genuinely shows the whole cloud on this tab.
        self.assertIsNone(f["allShowsOnly"])
        # 4 category proxies + 1 connector proxy, all device-independent (None).
        proxies = [t for t in payload["traces"] if t.get("showlegend") is True]
        self.assertEqual(len(proxies), 5)

    def test_nonpositive_ratio_is_unplottable(self):
        rows = [_conc_row("T", "c", v1_rank=1, v2_rank=1, csr=0.0, ds=0.2, led=0.1)]
        payload = concordance_3d_plot_payload(pd.DataFrame(rows))
        # The single target is unplottable -> no device point traces.
        self.assertEqual(payload["traces"], [])

    def test_terminal_axis_falls_back_to_nats_over_ln10_when_dex_absent(self):
        # _conc_row's fixture has no log_energy_delta_dex column (older-CSV
        # shape), so Z must be derived from the natural-log log_energy_delta
        # by dividing once by ln(10).
        payload = concordance_3d_plot_payload(self._frame())
        markers = next(t for t in payload["traces"]
                       if t["type"] == "scatter3d" and t["mode"] == "markers"
                       and t["x"] and t["x"][0] is not None)
        self.assertAlmostEqual(markers["z"][0], 0.1 / math.log(10))
        self.assertAlmostEqual(markers["z"][3], 2.0 / math.log(10))

    def test_terminal_axis_prefers_precomputed_dex_column(self):
        # When log_energy_delta_dex is present it must be used as-is -- never
        # divided by ln(10) again. led=999 is deliberately far from the dex
        # value so a double-conversion bug would fail this assertion loudly.
        row = _conc_row("T", "c", v1_rank=1, v2_rank=1, csr=1.0, ds=0.2, led=999.0)
        row["log_energy_delta_dex"] = 0.75
        payload = concordance_3d_plot_payload(pd.DataFrame([row]))
        markers = next(t for t in payload["traces"]
                       if t["type"] == "scatter3d" and t["mode"] == "markers"
                       and t["x"] and t["x"][0] is not None)
        self.assertAlmostEqual(markers["z"][0], 0.75)


class V2KeyTailTests(unittest.TestCase):
    """Axis/hover labels must never cut a record key mid-token."""

    def test_typical_key_shown_whole(self):
        self.assertEqual(
            _v2_key_tail("irradiation:10843:11297"),
            "irradiation:10843:11297",
        )

    def test_long_key_cut_at_token_boundary_with_ellipsis(self):
        tail = _v2_key_tail("campaign:2026:irradiation:1084355:1129777")
        self.assertTrue(tail.startswith("…"))
        # No mid-token fragment like 'iation:...': the cut lands after a ':'.
        self.assertEqual(tail, "…irradiation:1084355:1129777")

    def test_target_label_keeps_typical_key_whole(self):
        label = _v2_target_label({
            "target_stress_record_key": "irradiation:10843:11297",
            "target_event_type": "SEB",
        })
        self.assertEqual(label, "SEB · irradiation:10843:11297")


class LegendRowLayoutTests(unittest.TestCase):
    """Tabs stacking a two-line title + horizontal legend need a top margin
    that can hold both; 88px rendered them on top of each other."""

    def _v2_frame(self) -> pd.DataFrame:
        return pd.DataFrame([_v2_row("DUT-A", 1, "strong_overlap", "k-a1")])

    def test_v2_cartesian_payloads_reserve_title_and_legend_room(self):
        for payload in (
            v2_interval_overlap_plot_payload(self._v2_frame()),
            v2_severity_parity_plot_payload(self._v2_frame()),
            v2_overlap_summary_plot_payload(self._v2_frame()),
        ):
            layout = payload["layout"]
            self.assertGreaterEqual(layout["margin"]["t"], 170)
            self.assertEqual(layout["title"]["yanchor"], "top")
            self.assertEqual(layout["title"]["y"], 1.0)
            if "legend" in layout:
                self.assertEqual(layout["legend"]["yanchor"], "top")
                self.assertLessEqual(layout["legend"]["y"], layout["title"]["y"])


class TemplateDeviceFilterHonestyTests(unittest.TestCase):
    """The 'All devices' fallback on per-device tabs must be labeled, not
    silent: the dropdown said All while the chart showed one device."""

    def test_template_marks_per_device_fallback_in_title_and_note(self):
        self.assertIn("per-device view", HTML_TEMPLATE)
        self.assertIn("first of", HTML_TEMPLATE)
        self.assertIn("noteFor", HTML_TEMPLATE)

    def test_template_applies_per_device_axis_ranges(self):
        self.assertIn("xaxis.range", HTML_TEMPLATE)
        self.assertIn("rangeAll", HTML_TEMPLATE)


class DexSeriesTests(unittest.TestCase):
    """Unit coverage for the single log_energy_delta -> dex conversion point."""

    def test_converts_nats_when_dex_column_missing(self):
        frame = pd.DataFrame({"log_energy_delta": [0.0, 2.0, math.log(10)]})
        out = dex_series(frame, "log_energy_delta_dex", "log_energy_delta")
        self.assertAlmostEqual(out.iloc[0], 0.0)
        self.assertAlmostEqual(out.iloc[1], 2.0 / math.log(10))
        self.assertAlmostEqual(out.iloc[2], 1.0)

    def test_prefers_precomputed_dex_over_converting_nats(self):
        frame = pd.DataFrame({
            "log_energy_delta": [999.0],
            "log_energy_delta_dex": [0.75],
        })
        out = dex_series(frame, "log_energy_delta_dex", "log_energy_delta")
        self.assertAlmostEqual(out.iloc[0], 0.75)

    def test_falls_back_per_row_when_dex_column_has_gaps(self):
        frame = pd.DataFrame({
            "log_energy_delta": [2.0, 4.0],
            "log_energy_delta_dex": [0.75, None],
        })
        out = dex_series(frame, "log_energy_delta_dex", "log_energy_delta")
        self.assertAlmostEqual(out.iloc[0], 0.75)
        self.assertAlmostEqual(out.iloc[1], 4.0 / math.log(10))


class EnergyDeltaPlotPayloadDexTests(unittest.TestCase):
    def _row(self, led: float) -> dict:
        return {
            "damage_signature_distance": 0.2,
            "log_energy_delta": led,
            "energy_density_ratio": 1.0,
            "target_device_label": "DUT-A",
            "target_event_type": "SEB",
            "target_filename": "target.csv",
            "candidate_device_label": "DUT-A",
            "candidate_source": "avalanche",
            "candidate_rank": 1,
            "candidate_status": "measured_damage_candidate",
        }

    def test_y_axis_uses_dex_not_nats(self):
        comparisons = pd.DataFrame([self._row(2.0)])
        payload = energy_delta_plot_payload(comparisons)
        trace = next(t for t in payload["traces"]
                     if t["type"] == "scatter3d" and t.get("y") and t["y"][0] is not None)
        self.assertAlmostEqual(trace["y"][0], 2.0 / math.log(10))


if __name__ == "__main__":
    unittest.main()
