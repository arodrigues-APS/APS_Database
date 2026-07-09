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
  * Candidate destruction-boundary cells choose a dominant terminal-energy
    basis family, and both bracket edges and counts are computed only from
    rows in that family.  Rows with unknown outcomes are excluded from both
    bracket sides and counted separately.
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


# ── Regime compatibility (shared v1/v2 prior layer) ──
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
# status.  The pure function below mirrors the SQL seed, which lives in
# schema/025 since 2026-07-02 (shared v1/v2 prior layer; 028 only consumes it).

from collections import namedtuple

# path_penalty is the v1-distance-scale penalty consumed by the Phase-C
# mask-aware ranker (sourced from this table instead of the deprecated
# event-type-keyed stress_mechanism_compatibility). Mapping mirrors the v1
# constants: first_order 0.15, secondary 0.25, gate/cumulative analogs 0.50,
# questionable/mismatch 0.75.
RegimeMatch = namedtuple(
    "RegimeMatch",
    ["match_class", "status_ceiling", "preference", "rationale", "path_penalty"],
    defaults=(0.75,),
)

# match_class vocabulary: first_order_analog < secondary_analog <
# cumulative_analog < analog_questionable < mechanism_mismatch.
_REGIME_COMPATIBILITY = {
    "heavy_ion_hard_collapse_seb": {
        "avalanche_hard_collapse": RegimeMatch(
            "first_order_analog", None, 1,
            "Hard-collapse heavy-ion SEB matches inductive avalanche field-collapse burnout.",
            0.15),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit high-power pulse shares thermal runaway with a less direct topology.",
            0.25),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "secondary_analog", None, 2,
            "Repetitive avalanche reaches similar collapse but is a multi-pulse stimulus.",
            0.25),
        "sc_low_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Low-collapse SC does not match a hard-collapse heavy-ion SEB.",
            0.75),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "No collapse-matched electrical analog seeded for this heavy-ion SEB.",
            0.75),
    },
    "proton_low_collapse_seb": {
        "sc_low_collapse": RegimeMatch(
            "first_order_analog", None, 1,
            "Low-collapse proton SEB matches short-circuit low-collapse stress (proton diagnostic).",
            0.15),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit candidate; collapse is higher than the near-zero proton SEB target.",
            0.25),
        "avalanche_hard_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche hard collapse does not match near-zero proton SEB collapse.",
            0.75),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "Weak analog for low-collapse proton SEB.",
            0.75),
    },
    "proton_high_field_seb": {
        "avalanche_hard_collapse": RegimeMatch(
            "secondary_analog", None, 2,
            "High-field proton SEB with collapse; avalanche is a partial field-collapse analog.",
            0.25),
        "sc_high_power_short_pulse": RegimeMatch(
            "secondary_analog", None, 2,
            "Short-circuit high-power pulse is a partial analog for high-field proton SEB.",
            0.25),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "Inspect high-field proton SEB manually.",
            0.75),
    },
    "selci_gate_coupled": {
        # SELC-I is gate-oxide leakage.  Short-circuit stresses the gate; avalanche
        # (drain-source UIS) does not.  Avalanche is therefore a mechanism mismatch
        # here regardless of repetition: the earlier SC->avalanche flip was a
        # repetition/pool-size artifact (the only repetitive candidates in the data
        # are avalanche), not physics.  See the 2026-06-26 handoff.
        "repetitive_sc_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 1,
            "Repetitive SC gate stress is the gate-coupled, cumulative analog for SELC-I leakage.",
            0.50),
        "sc_high_power_short_pulse": RegimeMatch(
            "gate_coupled_analog", "analog_questionable", 1,
            "Short-circuit stresses the gate oxide implicated in SELC-I leakage.",
            0.50),
        "sc_low_collapse": RegimeMatch(
            "gate_coupled_analog", "analog_questionable", 1,
            "Short-circuit stresses the gate oxide implicated in SELC-I leakage.",
            0.50),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Repetitive avalanche is a drain-source stress with no gate-oxide coupling for SELC-I.",
            0.75),
        "avalanche_hard_collapse": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I.",
            0.75),
        "avalanche_noncatastrophic": RegimeMatch(
            "mechanism_mismatch", "analog_questionable", 4,
            "Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I.",
            0.75),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "SELC-I needs gate-coupled (short-circuit) evidence before any strong status.",
            0.75),
    },
    "selcii_drain_source_cumulative": {
        "repetitive_sc_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 2,
            "Cumulative drain-source leakage weakly tracked by repetitive electrical overstress.",
            0.50),
        "repetitive_avalanche_cumulative": RegimeMatch(
            "cumulative_analog", "analog_questionable", 2,
            "Cumulative drain-source leakage weakly tracked by repetitive avalanche overstress.",
            0.50),
        "any": RegimeMatch(
            "analog_questionable", "analog_questionable", 3,
            "SELC-II is cumulative defect leakage without a strong single-pulse analog.",
            0.75),
    },
}

# Targets with no credible electrical analog fall through to this.
_REGIME_DEFAULT = RegimeMatch(
    "analog_questionable", "analog_questionable", 3,
    "No seeded regime-compatibility rule; requires manual analog review.",
    0.75)


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
    "mechanistic_energy_screening_only": 4,
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
    energy_rankable,
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
    if energy_rankable:
        return "mechanistic_energy_screening_only"
    return "mechanistic_inspect_manually"


def _is_repetitive(candidate_pulse_count):
    try:
        return candidate_pulse_count is not None and int(candidate_pulse_count) > 1
    except (TypeError, ValueError):
        return False


# ── R1 prep — candidate-side electrical destruction boundary ────────────────
#
# The planned R1 change replaces the candidate "critical severity" ratio
# (bulk terminal areal energy / Kosier U_SEB|U_SELC — a radiation TRIGGER
# threshold, structurally a far-miss for bulk pulses) with the candidate's
# fraction of its OWN electrical failure threshold: pulse energy over the
# device's measured destruction-boundary energy, Wu-2024 thermal-runaway
# model as the fallback FORM where no measured boundary exists.  These pure
# functions are the spec the boundary SQL view must mirror; they encode the
# review's degenerate-case rules (bracket inversion, minimum cell counts,
# right-censored destructive energy, energy-basis consistency, repetitive
# exclusion) so the SQL cannot silently discard the richest cells.

# Wu et al. 2024 (Electronics 13(3):996) behavior model, pinned from the
# in-repo PDF (docs/relevant_papers/Linking stress types/electronics-13-00996.pdf).
# One thermal-runaway criterion spans short-circuit withstand and avalanche
# failure: junction temperature T = P_loss * Z_th(t) + T_case; the failure
# switch latches once T exceeds T_CRIT sustained for a short fitted delay
# t_FD.  T_CRIT characterizes source-metal (Al) melting as the failure
# precursor; the RC thermal-ladder values and t_FD are DEVICE-FITTED
# (their Table A1, C2M0080120D) and must not be copied across devices —
# measured boundaries stay authoritative, the model supplies the criterion
# form.  Temperatures in kelvin.
WU_2024_TCRIT_AL_MELT_K = 933.5           # Al melting point, their T_CRIT anchor
WU_2024_SIC_INTRINSIC_LIMIT_K = 1543.0    # 4H-SiC n_i = 1e16 cm^-3 at 1270 degC
# Literature junction-T estimates at avalanche failure they survey: 510-948 degC.
WU_2024_AVALANCHE_FAILURE_TJ_BAND_K = (783.0, 1221.0)

# Minimum survived / destructive records before a boundary cell may gate or
# rank anything (screening assumptions, not fitted constants).
BOUNDARY_MIN_SURVIVED_COUNT = 3
BOUNDARY_MIN_DESTRUCTIVE_COUNT = 3
BOUNDARY_UNKNOWN_OUTCOME_NOTE = "unknown_outcome_rows_excluded_from_bracket"

# Boundary cells are built from single-pulse regimes only.  A per-pulse energy
# bracket is meaningless for repetitive sequences (damage accrues over the
# sequence), so repetitive candidates fall to missing_interval until the
# cumulative dose/pulse-count axis exists.
REPETITIVE_CANDIDATE_REGIMES = {
    "repetitive_avalanche_cumulative",
    "repetitive_sc_cumulative",
}

_ENERGY_BASIS_FAMILIES = (
    ("commanded_or_stored", "commanded_or_stored"),
    ("proxy", "proxy"),
    ("integrated", "integrated"),
)


def survived_evidence(response_reversibility, avalanche_outcome=None) -> bool:
    """Whether a row has positive evidence for the survived bracket side.

    Destructive evidence is still ``response_reversibility ==
    'destructive_or_catastrophic'``.  Everything that is neither destructive
    nor positively survived is unknown and must be excluded from both bracket
    sides, not silently counted as survived.

    A destructive row is never survived evidence, even when it also carries a
    non-fail avalanche outcome (contradictory metadata: e.g. a catastrophic
    flag from waveform extraction next to a 'pass' outcome string).  Without
    this guard such a row would sit on BOTH bracket sides.
    """
    if response_reversibility == "destructive_or_catastrophic":
        return False
    if response_reversibility == "post_iv_measured":
        return True
    if avalanche_outcome is None:
        return False
    return "fail" not in str(avalanche_outcome).lower()


def energy_basis_family(basis):
    """Coarse family of a stress_energy/terminal-energy basis tag.

    A boundary bracket mixing integrated Joules with commanded 1/2*L*I^2
    Joules inherits a hidden multiplicative bias, so the numerator and the
    boundary cell must agree at the family level before a failure fraction
    may gate anything (review fix 5).
    """
    text = (basis or "").strip().lower()
    if not text:
        return "missing"
    for token, family in _ENERGY_BASIS_FAMILIES:
        if token in text:
            return family
    return "other"


def destruction_boundary_interval(
    max_survived_energy_j,
    min_destructive_energy_j,
    survived_count,
    destructive_count,
    min_survived_count=BOUNDARY_MIN_SURVIVED_COUNT,
    min_destructive_count=BOUNDARY_MIN_DESTRUCTIVE_COUNT,
    unknown_outcome_count=0,
):
    """Bracket the electrical destruction boundary for one boundary cell.

    Returns a dict with ``low_j``/``high_j`` (the bracket, always ascending),
    ``inverted`` (unit-to-unit spread put the max survived energy above the
    min destructive energy — the bracket is emitted, flagged, NOT discarded),
    ``usable`` (allowed to gate/rank; False leaves it a visible
    missing_interval downstream), and visible ``blockers``/``notes``.

    Censoring direction: destructive-pulse energy is truncated at failure, so
    ``min_destructive_energy_j`` is a lower bound of a lower bound — it can
    only widen the bracket downward (conservative); noted, never blocked.
    """
    low_bound = finite_float(max_survived_energy_j)
    if low_bound is not None and low_bound <= 0.0:
        low_bound = None
    high_bound = finite_float(min_destructive_energy_j)
    if high_bound is not None and high_bound <= 0.0:
        high_bound = None

    blockers = []
    notes = []
    inverted = False
    low_j = high_j = None

    if low_bound is None and high_bound is None:
        blockers.append("destruction_boundary_missing")
    elif low_bound is None:
        blockers.append("destruction_boundary_one_sided_destructive_only")
    elif high_bound is None:
        blockers.append("destruction_boundary_one_sided_survived_only")
    else:
        low_j, high_j = min(low_bound, high_bound), max(low_bound, high_bound)
        if low_bound > high_bound:
            inverted = True
            notes.append("destruction_boundary_brackets_inverted_unit_spread")

    if high_bound is not None:
        notes.append("destructive_energy_right_censored_lower_bound")

    def _count(value):
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0

    if _count(unknown_outcome_count) > 0:
        notes.append(BOUNDARY_UNKNOWN_OUTCOME_NOTE)

    if low_bound is not None and _count(survived_count) < min_survived_count:
        blockers.append("destruction_boundary_insufficient_survived_count")
    if high_bound is not None and _count(destructive_count) < min_destructive_count:
        blockers.append("destruction_boundary_insufficient_destructive_count")

    return {
        "low_j": low_j,
        "high_j": high_j,
        "inverted": inverted,
        "usable": low_j is not None and high_j is not None and not blockers,
        "blockers": blockers,
        "notes": notes,
    }


def candidate_failure_fraction(
    candidate_energy_j,
    boundary,
    candidate_energy_basis=None,
    boundary_energy_basis=None,
    candidate_regime=None,
):
    """Candidate severity as a fraction of its OWN destruction boundary.

    ``fraction_point`` divides the pulse energy by the geometric mean of the
    boundary bracket; ``fraction_low``/``fraction_high`` divide by the bracket
    ends (dividing by the HIGH end gives the LOW fraction).  Fractions are
    populated for display whenever a bracket exists, but ``usable`` is False —
    and downstream gating must treat the axis as missing_interval — when the
    boundary itself is unusable, the energy-basis families disagree, or the
    candidate is a repetitive sequence.
    """
    blockers = list(boundary.get("blockers") or [])
    notes = list(boundary.get("notes") or [])

    energy = finite_float(candidate_energy_j)
    if energy is not None and energy <= 0.0:
        energy = None
    if energy is None:
        blockers.append("candidate_energy_missing")

    if candidate_regime in REPETITIVE_CANDIDATE_REGIMES:
        blockers.append("boundary_repetitive_regime_excluded")

    if candidate_energy_basis is not None or boundary_energy_basis is not None:
        candidate_family = energy_basis_family(candidate_energy_basis)
        boundary_family = energy_basis_family(boundary_energy_basis)
        if candidate_family != boundary_family:
            blockers.append("boundary_energy_basis_family_mismatch")

    low_j = boundary.get("low_j")
    high_j = boundary.get("high_j")
    fraction_low = fraction_point = fraction_high = None
    if energy is not None and low_j and high_j:
        fraction_low = energy / high_j
        fraction_high = energy / low_j
        fraction_point = energy / math.sqrt(low_j * high_j)

    return {
        "fraction_low": fraction_low,
        "fraction_point": fraction_point,
        "fraction_high": fraction_high,
        "usable": fraction_point is not None and not blockers,
        "blockers": blockers,
        "notes": notes,
    }
