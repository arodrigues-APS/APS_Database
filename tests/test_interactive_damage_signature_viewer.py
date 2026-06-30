import unittest

import pandas as pd

from data_processing_scripts.create_interactive_damage_signature_viewer import (
    KOSIER_2026_SELC_CRITICAL_J_CM2,
    energy_balance_plot_payload,
    irradiation_energy_summary,
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


if __name__ == "__main__":
    unittest.main()
