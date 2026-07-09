import unittest

from data_processing_scripts import proxy_viz_palette as p
from data_processing_scripts.create_interactive_damage_signature_viewer import V3_COMPONENTS


def _rgb(hex_color):
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i + 2], 16) / 255.0 for i in (0, 2, 4))


def _linear(channel):
    return channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4


def _luminance(hex_color):
    r, g, b = _rgb(hex_color)
    return 0.2126 * _linear(r) + 0.7152 * _linear(g) + 0.0722 * _linear(b)


def _contrast(hex_a, hex_b="#ffffff"):
    la = _luminance(hex_a)
    lb = _luminance(hex_b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


class ProxyVizPaletteTests(unittest.TestCase):
    def test_shared_palette_sets_have_light_surface_contrast(self):
        sets = {
            "source": p.SOURCE_COLORS,
            "event": p.EVENT_TYPE_COLORS,
            "claim": p.CLAIM_STATUS_COLORS,
            "overlap": p.OVERLAP_COLORS,
            "v3": p.V3_COMPONENT_COLORS,
        }
        for set_name, colors in sets.items():
            for key, color in colors.items():
                with self.subTest(set=set_name, key=key, color=color):
                    self.assertGreaterEqual(_contrast(color), 3.0)

    def test_v3_component_order_matches_palette_and_avoids_red_green_adjacency(self):
        order = [name for name, _share, _sq in V3_COMPONENTS]
        self.assertEqual(order, list(p.V3_COMPONENT_COLORS.keys()))
        adjacent = list(zip(order, order[1:]))
        self.assertNotIn(("failure fraction", "terminal energy"), adjacent)
        self.assertNotIn(("terminal energy", "failure fraction"), adjacent)

    def test_event_palette_order_avoids_old_red_green_adjacency(self):
        colors = list(p.EVENT_TYPE_COLORS.values())
        adjacent = list(zip(colors, colors[1:]))
        old_red_green = {"#e45756", "#54a24b"}
        for pair in adjacent:
            self.assertNotEqual(set(pair), old_red_green)
        self.assertNotIn("#e45756", colors)
        self.assertNotIn("#54a24b", colors)

    def test_overlap_ramp_is_single_hue_family_with_direct_labels_required(self):
        self.assertEqual(
            list(p.OVERLAP_COLORS.keys()),
            ["strong_overlap", "partial_overlap", "near_miss", "far_miss", "missing_interval"],
        )
        # The ramp is deliberately blue-gray, not green-yellow-red; the labels
        # carry the ordinal semantics and prevent hue-only interpretation.
        for color in p.OVERLAP_COLORS.values():
            r, g, b = _rgb(color)
            self.assertLessEqual(abs(g - b), 0.20)


if __name__ == "__main__":
    unittest.main()
