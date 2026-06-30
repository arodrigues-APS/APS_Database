#!/usr/bin/env python3
"""
Mechanistic energy-proxy feature helpers (Phase 1 — feature correctness).

This module is the Python source-of-truth for the energy-equivalence feature
math.  ``schema/028_mechanistic_energy_proxy.sql`` mirrors these functions in
SQL (the ``stress_energy_equivalence_features`` view); the unit tests pin the
Python and the SQL is kept algebraically identical.  Nothing here ranks
candidates — it only builds the per-record feature vector described in
``docs/mechanistic_energy_proxy_rollout_plan_2026-06-26.md``.

Design rules enforced here (see the rollout plan):

  * Critical-energy *severity* is a target-side quantity for irradiation
    (stored depletion field energy vs Kosier U_SEB/U_SELC) and a *separately
    named* bulk terminal areal-energy ratio for SC/avalanche candidates.  The
    two must never share a column name — an SC pulse has no stored depletion
    field energy.
  * The full-active-volume irradiation "energy density" is a bulk-equivalent
    descriptor, never a track-core density.  Track-core density is computed
    explicitly from an assumed track-core radius and is basis-tagged.
  * SEB is not one proxy family.  Regimes split by measured collapse and
    beam/LET, not by the ``SEB`` label alone.
  * Uncertainty is expressed as deterministic lower/nominal/upper bands.  The
    interval-overlap helpers exist for the (later) v2 ranker; the feature view
    only emits the bands.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict


# ── Physical constants (authoritative; mirror Kosier 2026 Table II) ──────────
SEB_CRITICAL_J_CM2 = 207e-6
SELC_CRITICAL_J_CM2 = 60e-6
MEV_TO_J = 1.602176634e-13
# 4H-SiC mass density.  Used only for the surface-LET track-core density
# (LET * density gives deposited energy per unit track length).
SIC_DENSITY_MG_CM3 = 3210.0

_PROTON_ALIASES = {"p", "proton", "protons", "h", "h+", "h1", "1h"}


@dataclass(frozen=True)
class EnergyEquivalenceSettings:
    """First-pass severity/uncertainty constants.

    Every value here is a documented screening assumption, not a fitted
    constant (the measured truth set is far too small to fit).  See the
    rollout plan's open questions §1-§3.
    """

    setting_name: str = "default"
    # Track-core radius band [um].  The ion charge column is ~0.1 um; the wide
    # upper bound keeps the localization mismatch a sensitivity range, not a
    # point claim.
    default_track_core_radius_um: float = 0.1
    track_core_radius_low_um: float = 0.05
    track_core_radius_high_um: float = 0.5
    # A "hard collapse" event has Vds collapse fraction at/above this.
    collapse_hard_threshold: float = 0.5
    # Terminal-energy log-sigma by basis (natural-log multiplicative 1-sigma).
    terminal_energy_log_sigma_integrated: float = 0.20
    terminal_energy_log_sigma_commanded: float = 0.41
    terminal_energy_log_sigma_censored: float = 0.69
    # Active-area log-sigma by geometry confidence.
    active_area_log_sigma_measured: float = 0.20
    active_area_log_sigma_estimated: float = 0.69
    # Net-doping log-sigma by basis.  The reachthrough estimate is known to run
    # high (≈1.6x), hence the wider estimated band (ln 1.6 ≈ 0.47).
    doping_log_sigma_measured: float = 0.20
    doping_log_sigma_estimated: float = 0.47
    # Geometry confidence at/above which area uncertainty uses the measured band.
    geometry_confidence_measured_min: float = 0.5
    # Reserved for the v2 ranker; carried so settings travel together.
    same_regime_required_for_primary: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def finite_float(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def is_proton(ion_species) -> bool:
    s = (ion_species or "").strip().lower()
    if not s:
        return False
    return s in _PROTON_ALIASES or "proton" in s


# ── Geometry / areal energy ─────────────────────────────────────────────────

def active_area_cm2(active_volume_cm3, active_thickness_um):
    """Electrically-active cross-sectional area from active volume / thickness."""
    volume = finite_float(active_volume_cm3)
    thickness_um = finite_float(active_thickness_um)
    if volume is None or volume <= 0.0 or thickness_um is None or thickness_um <= 0.0:
        return None
    return volume / (thickness_um * 1e-4)


def terminal_areal_energy_j_cm2(terminal_energy_j, area_cm2):
    """Bulk terminal areal-energy loading [J/cm^2] for an electrical candidate."""
    energy = finite_float(terminal_energy_j)
    area = finite_float(area_cm2)
    if energy is None or energy <= 0.0 or area is None or area <= 0.0:
        return None
    return energy / area


def critical_ratio(areal_energy_j_cm2, critical_j_cm2):
    """Express an areal energy as a multiple of a critical areal energy."""
    areal = finite_float(areal_energy_j_cm2)
    critical = finite_float(critical_j_cm2)
    if areal is None or critical is None or critical <= 0.0:
        return None
    return areal / critical


# ── Track-core (localization-aware) density ─────────────────────────────────

def track_core_volume_cm3(radius_um, path_length_um):
    radius_um = finite_float(radius_um)
    path_length_um = finite_float(path_length_um)
    if (radius_um is None or radius_um <= 0.0
            or path_length_um is None or path_length_um <= 0.0):
        return None
    radius_cm = radius_um * 1e-4
    length_cm = path_length_um * 1e-4
    return math.pi * radius_cm ** 2 * length_cm


def track_core_energy_density_from_deposited(single_particle_deposited_energy_j,
                                             radius_um, path_length_um):
    """Track-core density from a per-particle deposited energy and core geometry."""
    energy = finite_float(single_particle_deposited_energy_j)
    volume = track_core_volume_cm3(radius_um, path_length_um)
    if energy is None or energy <= 0.0 or volume is None or volume <= 0.0:
        return None
    return energy / volume


def track_core_energy_density_from_let(let_mev_cm2_mg, radius_um,
                                       density_mg_cm3=SIC_DENSITY_MG_CM3):
    """Track-core density [J/cm^3] from surface LET and an assumed core radius.

    LET * density is deposited energy per unit track length; dividing by the
    core cross-section gives an energy density that is independent of path
    length.  This uses *surface* LET, so it is an upper-region estimate — the
    basis tag must say so until LET-at-depletion (Method 6) lands.
    """
    let_mev = finite_float(let_mev_cm2_mg)
    radius_um = finite_float(radius_um)
    density = finite_float(density_mg_cm3)
    if (let_mev is None or let_mev <= 0.0 or radius_um is None or radius_um <= 0.0
            or density is None or density <= 0.0):
        return None
    radius_cm = radius_um * 1e-4
    core_area_cm2 = math.pi * radius_cm ** 2
    energy_per_length_mev_cm = let_mev * density  # MeV/cm
    return energy_per_length_mev_cm * MEV_TO_J / core_area_cm2


# ── Uncertainty intervals and overlap (overlap is for the later v2 ranker) ──

def log_interval(nominal, log_sigma, k=1.0):
    """Multiplicative lower/upper band: nominal * exp(±k*sigma)."""
    nominal = finite_float(nominal)
    sigma = finite_float(log_sigma)
    if nominal is None or nominal <= 0.0 or sigma is None or sigma < 0.0:
        return (None, None)
    factor = math.exp(k * sigma)
    return (nominal / factor, nominal * factor)


def combine_log_sigmas(*sigmas):
    vals = [finite_float(s) for s in sigmas]
    vals = [s for s in vals if s is not None and s >= 0.0]
    if not vals:
        return None
    return math.sqrt(sum(s * s for s in vals))


def terminal_energy_log_sigma(basis, settings):
    """Pick a terminal-energy log-sigma from the energy basis tag."""
    b = (basis or "").lower()
    if "proxy" in b or "commanded_or_stored" in b:
        # commanded/stored is a recipe, not an integrated measurement
        if "commanded_or_stored" in b:
            return settings.terminal_energy_log_sigma_commanded
        return settings.terminal_energy_log_sigma_censored
    if "integrated" in b:
        return settings.terminal_energy_log_sigma_integrated
    return settings.terminal_energy_log_sigma_censored


def active_area_log_sigma(geometry_confidence, settings):
    conf = finite_float(geometry_confidence)
    if conf is not None and conf >= settings.geometry_confidence_measured_min:
        return settings.active_area_log_sigma_measured
    return settings.active_area_log_sigma_estimated


def doping_log_sigma(net_doping_basis, settings):
    b = (net_doping_basis or "").lower()
    if "estimate" in b or "reachthrough" in b:
        return settings.doping_log_sigma_estimated
    return settings.doping_log_sigma_measured


def depletion_ratio_interval(ratio, net_doping_basis, settings, k=1.0):
    """Lower/upper band on a stored-energy critical ratio.

    U_stored ∝ √N, so a log-sigma on doping maps to half that on the ratio.
    """
    sigma_n = doping_log_sigma(net_doping_basis, settings)
    return log_interval(ratio, 0.5 * sigma_n, k=k)


def terminal_ratio_interval(ratio, energy_basis, geometry_confidence, settings, k=1.0):
    """Lower/upper band on a candidate terminal areal-energy critical ratio."""
    sigma = combine_log_sigmas(
        terminal_energy_log_sigma(energy_basis, settings),
        active_area_log_sigma(geometry_confidence, settings),
    )
    return log_interval(ratio, sigma, k=k)


def intervals_overlap(low1, high1, low2, high2):
    vals = [finite_float(v) for v in (low1, high1, low2, high2)]
    if any(v is None for v in vals):
        return None
    low1, high1, low2, high2 = vals
    return low1 <= high2 and low2 <= high1


def overlap_class(low1, high1, low2, high2):
    """Classify the overlap of two intervals (used by the v2 ranker).

    Returns one of strong_overlap / partial_overlap / near_miss / far_miss /
    missing_interval.  "strong" means the overlap spans at least half of the
    narrower interval.  A disjoint pair is "near" if the gap is no wider than
    the narrower interval, else "far".
    """
    vals = [finite_float(v) for v in (low1, high1, low2, high2)]
    if any(v is None for v in vals):
        return "missing_interval"
    low1, high1, low2, high2 = vals
    if high1 < low1 or high2 < low2:
        return "missing_interval"
    width1 = high1 - low1
    width2 = high2 - low2
    denom = min(width1, width2)
    if low1 <= high2 and low2 <= high1:
        overlap = min(high1, high2) - max(low1, low2)
        if denom <= 0.0:
            return "strong_overlap"
        return "strong_overlap" if overlap / denom >= 0.5 else "partial_overlap"
    gap = max(low1, low2) - min(high1, high2)
    if denom <= 0.0:
        return "far_miss"
    return "near_miss" if gap <= denom else "far_miss"


# ── Mechanistic regime classification ───────────────────────────────────────

# Regime vocabularies (kept in sync with the SQL CASE and the rollout plan).
TARGET_REGIMES = {
    "heavy_ion_hard_collapse_seb",
    "proton_low_collapse_seb",
    "proton_high_field_seb",
    "selci_gate_coupled",
    "selcii_drain_source_cumulative",
    "mixed_single_event",
    "tid_dd_cumulative",
    "unknown_single_event",
}
CANDIDATE_REGIMES = {
    "avalanche_hard_collapse",
    "avalanche_noncatastrophic",
    "sc_high_power_short_pulse",
    "sc_low_collapse",
    "repetitive_avalanche_cumulative",
    "repetitive_sc_cumulative",
    "unknown_electrical_proxy",
}


def classify_mechanistic_regime(
    source,
    event_type=None,
    ion_species=None,
    vds_collapse_fraction=None,
    gate_delta_fraction=None,
    path_type=None,
    pulse_count_in_sequence=None,
    is_catastrophic=None,
    let_surface=None,
    settings=None,
):
    """Assign a measured-regime label before any ranking.

    The cardinal rule: do not key the proxy family on ``event_type='SEB'``
    alone.  SEB splits by measured collapse and proton-vs-heavy-ion beam so a
    low-collapse high-energy proton SEB is not forced through the same
    avalanche-favoring path as a hard-collapse heavy-ion SEB.
    """
    if settings is None:
        settings = EnergyEquivalenceSettings()
    src = (source or "").strip().lower()
    collapse = finite_float(vds_collapse_fraction)
    collapse_high = collapse is not None and collapse >= settings.collapse_hard_threshold
    pulses = pulse_count_in_sequence
    try:
        cumulative = pulses is not None and int(pulses) > 1
    except (TypeError, ValueError):
        cumulative = False

    if src == "irradiation":
        event = (event_type or "").strip().upper()
        proton = is_proton(ion_species)
        if event == "SEB":
            if proton and collapse_high:
                return "proton_high_field_seb"
            if proton:
                return "proton_low_collapse_seb"
            if collapse_high:
                return "heavy_ion_hard_collapse_seb"
            # Heavy ion with no/low collapse evidence stays ambiguous on
            # purpose; a blocker flags the missing signature.
            return "unknown_single_event"
        if event == "SELCI":
            return "selci_gate_coupled"
        if event == "SELCII":
            return "selcii_drain_source_cumulative"
        if event == "MIXED":
            return "mixed_single_event"
        if proton:
            return "tid_dd_cumulative"
        return "unknown_single_event"

    if src == "avalanche":
        if cumulative:
            return "repetitive_avalanche_cumulative"
        if collapse_high or bool(is_catastrophic):
            return "avalanche_hard_collapse"
        return "avalanche_noncatastrophic"

    if src == "sc":
        if cumulative:
            return "repetitive_sc_cumulative"
        if collapse_high:
            return "sc_high_power_short_pulse"
        return "sc_low_collapse"

    return "unknown_electrical_proxy"


# ── Regime compatibility (Phase 2 — priors only, consumed by no ranker yet) ──
#
# Which candidate regime is a credible analog for which target regime, keyed on
# *measured* regime (not the event-type label).  This is a reviewable prior,
# the regime-granular successor to stress_mechanism_compatibility, and it is
# deliberately seeded conservatively:
#
#   * proton low-collapse SEB prefers SHORT-CIRCUIT, not avalanche (the proton
#     diagnostic: near-zero collapse moves 43/44 events to SC);
#   * heavy-ion hard-collapse SEB prefers AVALANCHE field-collapse burnout;
#   * SELC-I / SELC-II are cumulative/latent — only repetitive electrical
#     overstress is even a weak analog, and always under an analog_questionable
#     ceiling.
#
# Lower ``preference`` ranks first.  ``status_ceiling`` caps any downstream v2
# status.  The pure function below mirrors the SQL seed in schema/028.

from collections import namedtuple

RegimeMatch = namedtuple(
    "RegimeMatch", ["match_class", "status_ceiling", "preference", "rationale"]
)

# match_class vocabulary: first_order_analog < secondary_analog <
# cumulative_analog < analog_questionable < mechanism_mismatch.
_REGIME_COMPATIBILITY = {
    "heavy_ion_hard_collapse_seb": {
        "avalanche_hard_collapse": RegimeMatch(
            "first_order_analog", None, 1,
            "Hard-collapse heavy-ion SEB matches inductive avalanche field-collapse burnout."),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit high-power pulse shares thermal runaway with a less direct topology."),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "secondary_analog", None, 2,
            "Repetitive avalanche reaches similar collapse but is a multi-pulse stimulus."),
        "sc_low_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Low-collapse SC does not match a hard-collapse heavy-ion SEB."),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "No collapse-matched electrical analog seeded for this heavy-ion SEB."),
    },
    "proton_low_collapse_seb": {
        "sc_low_collapse": RegimeMatch(
            "first_order_analog", None, 1,
            "Low-collapse proton SEB matches short-circuit low-collapse stress (proton diagnostic)."),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit candidate; collapse is higher than the near-zero proton SEB target."),
        "avalanche_hard_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche hard collapse does not match near-zero proton SEB collapse."),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "Weak analog for low-collapse proton SEB."),
    },
    "proton_high_field_seb": {
        "avalanche_hard_collapse": RegimeMatch(
            "secondary_analog", None, 2,
            "High-field proton SEB with collapse; avalanche is a partial field-collapse analog."),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit high-power pulse is a partial analog for high-field proton SEB."),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "Inspect high-field proton SEB manually."),
    },
    "selci_gate_coupled": {
        # SELC-I is gate-oxide leakage.  Short-circuit stresses the gate; avalanche
        # (drain-source UIS) does not.  Avalanche is therefore a mechanism mismatch
        # here regardless of repetition: the earlier SC->avalanche flip was a
        # repetition/pool-size artifact (the only repetitive candidates in the data
        # are avalanche), not physics.  See the 2026-06-26 handoff.
        "repetitive_sc_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 1,
            "Repetitive SC gate stress is the gate-coupled, cumulative analog for SELC-I leakage."),
        "sc_high_power_short_pulse": RegimeMatch(
            "gate_coupled_analog", "analog_questionable", 1,
            "Short-circuit stresses the gate oxide implicated in SELC-I leakage."),
        "sc_low_collapse": RegimeMatch(
            "gate_coupled_analog", "analog_questionable", 1,
            "Short-circuit stresses the gate oxide implicated in SELC-I leakage."),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Repetitive avalanche is a drain-source stress with no gate-oxide coupling for SELC-I."),
        "avalanche_hard_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I."),
        "avalanche_noncatastrophic": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I."),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "SELC-I needs gate-coupled (short-circuit) evidence before any strong status."),
    },
    "selcii_drain_source_cumulative": {
        "repetitive_sc_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 2,
            "Cumulative drain-source leakage weakly tracked by repetitive electrical overstress."),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 2,
            "Cumulative drain-source leakage weakly tracked by repetitive avalanche overstress."),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "SELC-II is cumulative defect leakage without a strong single-pulse analog."),
    },
}

# Targets with no credible electrical analog fall through to this.
_REGIME_DEFAULT = RegimeMatch(
    "analog_questionable", "analog_questionable", 3,
    "No seeded regime-compatibility rule; requires manual analog review.")


def regime_match_class(target_regime, candidate_regime):
    """Look up the seeded analog class for a (target, candidate) regime pair.

    Falls back to the target's ``any`` rule, then to a global
    analog_questionable default, so every pair resolves to a visible class.
    """
    by_target = _REGIME_COMPATIBILITY.get(target_regime)
    if by_target is None:
        return _REGIME_DEFAULT
    if candidate_regime in by_target:
        return by_target[candidate_regime]
    return by_target.get("any", _REGIME_DEFAULT)


# ── Phase 3 — v2 staged status / overlap classes (mirrors the SQL v2 view) ──
#
# No fitted weights and no single opaque score: each comparison resolves to a
# visible status plus per-axis overlap descriptors.  These pure functions are
# the source-of-truth that schema/028's stress_proxy_candidate_energy_v2 mirrors.

# Targets whose damage accrues cumulatively rather than in one destructive pulse.
CUMULATIVE_TARGET_REGIMES = {
    "selci_gate_coupled",
    "selcii_drain_source_cumulative",
    "tid_dd_cumulative",
}

# Lower ranks first.  mechanism_mismatch is ranked LAST (kept, not excluded).
MECH_STATUS_PRIORITY = {
    "mechanistic_measured_candidate": 1,
    "mechanistic_predicted_candidate": 2,
    "mechanistic_cumulative_candidate": 3,
    "mechanistic_waveform_candidate": 4,
    "mechanistic_analog_questionable": 5,
    "mechanistic_cross_device_screening_only": 6,
    "mechanistic_missing_damage_context": 6,
    "mechanistic_missing_energy_context": 6,
    "mechanistic_inspect_manually": 7,
    "mechanistic_regime_mismatch": 8,
}


def mechanistic_status_priority(status):
    return MECH_STATUS_PRIORITY.get(status, 9)


def terminal_energy_overlap_class(log_energy_delta):
    """Coarse overlap class on the terminal-energy log distance (comparable Joules)."""
    delta = finite_float(log_energy_delta)
    if delta is None:
        return "missing_interval"
    delta = abs(delta)
    if delta <= 0.5:
        return "strong_overlap"
    if delta <= 1.5:
        return "partial_overlap"
    if delta <= 3.0:
        return "near_miss"
    return "far_miss"


def localization_mismatch_class(mismatch_log10):
    """Class the log10 gap between candidate bulk density and target track-core density.

    Structurally large for every irradiation-vs-electrical pair, so this is a
    visible context descriptor, never a ranking gate.
    """
    mismatch = finite_float(mismatch_log10)
    if mismatch is None:
        return "missing"
    magnitude = abs(mismatch)
    if magnitude > 4.0:
        return "extreme_localized_vs_bulk"
    if magnitude > 2.0:
        return "large_localized_vs_bulk"
    if magnitude > 1.0:
        return "moderate_localized_vs_bulk"
    return "comparable"


def cumulative_exposure_overlap_class(target_regime, candidate_pulse_count):
    """Whether a cumulative target is paired with a repetitive candidate."""
    if target_regime not in CUMULATIVE_TARGET_REGIMES:
        return "not_applicable"
    try:
        repetitive = candidate_pulse_count is not None and int(candidate_pulse_count) > 1
    except (TypeError, ValueError):
        repetitive = False
    return "cumulative_present" if repetitive else "cumulative_missing"


def mechanistic_energy_candidate_status(
    regime_match_class_value,
    regime_status_ceiling,
    match_scope,
    target_has_energy_context,
    measured_comparability_status,
    prediction_comparability_status,
    target_regime,
    candidate_pulse_count,
    has_waveform,
):
    """Staged v2 status.  Post-IV damage stays the anchor; the regime ceiling
    caps optimistic statuses; mechanism_mismatch is ranked last, not dropped.
    """
    if regime_match_class_value == "mechanism_mismatch":
        return "mechanistic_regime_mismatch"
    if match_scope == "cross_device":
        return "mechanistic_cross_device_screening_only"
    if not target_has_energy_context:
        return "mechanistic_missing_energy_context"
    if (measured_comparability_status is None
            and prediction_comparability_status is None):
        return "mechanistic_missing_damage_context"

    cumulative_pair = (
        target_regime in CUMULATIVE_TARGET_REGIMES
        and _is_repetitive(candidate_pulse_count)
    )
    if regime_status_ceiling == "analog_questionable":
        if cumulative_pair:
            return "mechanistic_cumulative_candidate"
        return "mechanistic_analog_questionable"

    if measured_comparability_status in ("strong", "usable"):
        return "mechanistic_measured_candidate"
    if prediction_comparability_status in ("strong", "usable"):
        return "mechanistic_predicted_candidate"
    if cumulative_pair:
        return "mechanistic_cumulative_candidate"
    if has_waveform:
        return "mechanistic_waveform_candidate"
    return "mechanistic_inspect_manually"


def _is_repetitive(candidate_pulse_count):
    try:
        return candidate_pulse_count is not None and int(candidate_pulse_count) > 1
    except (TypeError, ValueError):
        return False
