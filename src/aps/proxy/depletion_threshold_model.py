#!/usr/bin/env python3
"""
Analytical stored-depletion-energy model for SiC SEB/SELC thresholds.

This module implements the one-dimensional critical energy storage/release
model from Kosier et al., TDMR 2026.  It is intentionally separate from:

  * radiation deposited energy, which is ion stopping power times fluence and
    material geometry, and
  * electrical terminal energy, which is waveform-integrated Vds * Id.

The model here estimates pre-strike electrostatic energy stored in the
reverse-biased depletion region.  The heavy ion supplies the transient track
resistance; the stored field energy is what the paper compares against the
SEB/SELC critical areal energy densities.
"""

from __future__ import annotations

import math


EPS0_F_PER_CM = 8.8541878128e-14
SIC_RELATIVE_PERMITTIVITY = 9.7
ELEMENTARY_CHARGE_C = 1.602176634e-19

KOSIER_2026_SEB_CRITICAL_J_CM2 = 207e-6
KOSIER_2026_SELC_CRITICAL_J_CM2 = 60e-6
KOSIER_2026_MODEL_BASIS = (
    "kosier_2026_1d_high_let_normal_incidence_full_epi_traversal"
)


def finite_float(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def sic_permittivity_f_per_cm(relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    relative_permittivity = finite_float(relative_permittivity)
    if relative_permittivity is None or relative_permittivity <= 0.0:
        return None
    return EPS0_F_PER_CM * relative_permittivity


def net_doping_from_reachthrough(voltage_v, depletion_width_um,
                                 relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    """
    Infer one-sided net epi doping from reachthrough voltage and depletion width.

    N = 2 * eps * V / (q * W^2)

    This is a practical estimator for datasets where we have rated voltage and
    an active SiC/drift-layer thickness estimate, but not measured epi doping.
    """
    voltage_v = finite_float(voltage_v)
    depletion_width_um = finite_float(depletion_width_um)
    eps = sic_permittivity_f_per_cm(relative_permittivity)
    if (
        voltage_v is None or voltage_v <= 0.0
        or depletion_width_um is None or depletion_width_um <= 0.0
        or eps is None
    ):
        return None
    width_cm = depletion_width_um * 1e-4
    return 2.0 * eps * voltage_v / (ELEMENTARY_CHARGE_C * width_cm ** 2)


def depletion_width_um(voltage_v, net_doping_cm3,
                       relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    """Return one-sided depletion width for the supplied reverse bias."""
    voltage_v = finite_float(voltage_v)
    net_doping_cm3 = finite_float(net_doping_cm3)
    eps = sic_permittivity_f_per_cm(relative_permittivity)
    if (
        voltage_v is None or voltage_v <= 0.0
        or net_doping_cm3 is None or net_doping_cm3 <= 0.0
        or eps is None
    ):
        return None
    width_cm = math.sqrt(
        2.0 * eps * voltage_v / (ELEMENTARY_CHARGE_C * net_doping_cm3)
    )
    return width_cm * 1e4


def stored_depletion_energy_areal_j_cm2(
        voltage_v, net_doping_cm3,
        relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    """
    Stored depletion-region areal energy density at reverse bias V.

    For the linear one-sided field profile used in the paper:

        U = (1/3) * sqrt(2 * eps * q * N * V^3)

    Units are J/cm^2.
    """
    voltage_v = finite_float(voltage_v)
    net_doping_cm3 = finite_float(net_doping_cm3)
    eps = sic_permittivity_f_per_cm(relative_permittivity)
    if (
        voltage_v is None or voltage_v <= 0.0
        or net_doping_cm3 is None or net_doping_cm3 <= 0.0
        or eps is None
    ):
        return None
    return (
        math.sqrt(
            2.0 * eps * ELEMENTARY_CHARGE_C * net_doping_cm3
            * voltage_v ** 3
        )
        / 3.0
    )


def critical_voltage_for_areal_energy(
        critical_energy_j_cm2, net_doping_cm3,
        relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    """
    Reverse voltage where stored depletion energy reaches the critical value.
    """
    critical_energy_j_cm2 = finite_float(critical_energy_j_cm2)
    net_doping_cm3 = finite_float(net_doping_cm3)
    eps = sic_permittivity_f_per_cm(relative_permittivity)
    if (
        critical_energy_j_cm2 is None or critical_energy_j_cm2 <= 0.0
        or net_doping_cm3 is None or net_doping_cm3 <= 0.0
        or eps is None
    ):
        return None
    return (
        (3.0 * critical_energy_j_cm2) ** 2
        / (2.0 * eps * ELEMENTARY_CHARGE_C * net_doping_cm3)
    ) ** (1.0 / 3.0)


def peak_field_v_cm(voltage_v, net_doping_cm3,
                    relative_permittivity=SIC_RELATIVE_PERMITTIVITY):
    """Peak junction electric field for the one-sided triangular profile."""
    width_um = depletion_width_um(
        voltage_v, net_doping_cm3,
        relative_permittivity=relative_permittivity,
    )
    voltage_v = finite_float(voltage_v)
    if width_um is None or width_um <= 0.0 or voltage_v is None:
        return None
    return 2.0 * voltage_v / (width_um * 1e-4)
