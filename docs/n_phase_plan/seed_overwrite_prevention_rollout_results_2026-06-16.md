# Seed Overwrite Prevention Rollout Results

Date completed: 2026-06-16

## Scope

This note records the rollout results for
`docs/seed_overwrite_prevention_plan_2026-06-16.md`.

The incident was that manually curated proton LET metadata was being reset to
`NULL` by nightly irradiation seeding. The rollout had two goals:

1. Ship the proton LET catalog values and remove the special `LET proton`
   chart bin so protons with numeric stopping-power metadata use the normal
   LET bands.
2. Make future irradiation run seeding safe by treating existing scientific
   numerics in the production database as authoritative unless an operator
   explicitly accepts seed-side conflicts.

## Implementation Summary

Implemented and committed in:

```text
96a1ac8 Prevent irradiation seed metadata overwrites
```

The rollout changed the irradiation seed policy in
`data_processing_scripts/seed_irradiation_campaigns.py`:

| Field group | Fields | Result |
| --- | --- | --- |
| Protected numerics | `let_surface`, `let_bragg_peak`, `range_um` | Fill blanks only; non-null DB/seed mismatches are conflicts. |
| Code-owned fields | `beam_type`, `notes` | Always updated from seed. |
| Identity fields | `ion_species`, `beam_energy_mev` | Never updated on existing rows by run upsert; still set only on insert or through legacy cleanup aliases. |

The seed now supports these operator modes:

| Mode | Behavior |
| --- | --- |
| Default | Insert missing runs, fill blank protected numerics, update code-owned fields, record conflicts, keep DB values, exit `0`. |
| `--audit-only` | Compute and print actions, write nothing, exit `0`. |
| `--strict` | Same conflict detection, but exits nonzero if any protected numeric conflict exists. |
| `--accept-seed-conflicts` | Intentionally overwrite protected numeric conflicts from seed and log before/after values. |

Additional rollout changes:

1. Added `seed_metadata_conflicts` for conflict audit records.
2. Threaded `cleanup_legacy_runs()` affected run ids into conflict detection
   so a run renamed during legacy cleanup does not produce a false conflict in
   the same pass.
3. Added `scripts/deploy_to_opt.sh` for explicit fast-forward deployment to
   `/opt/aps_database/APS_Database`.
4. Added source-scoped nightly preflight in
   `scripts/nightly_update_and_ingest.sh`; dirty tracked source files under
   `data_processing_scripts/`, `schema/`, `scripts/`, or `superset/` skip only
   `seed_irradiation_campaigns.py` and continue downstream.
5. Untracked the three regenerated `out/` artifacts named in the plan.
6. Removed the schema-side proton-forcing branch that produced
   `let_bin = 'LET proton'`.
7. Added focused tests in `tests/test_seed_irradiation_campaigns.py`.

## Deployment Evidence

Production checkout:

```text
/opt/aps_database/APS_Database
```

Deployment completed by fast-forwarding production from:

```text
e596cb9 -> 96a1ac8
```

The deploy helper handled the one-time tracked-to-untracked transition for the
dirty generated `out/` artifacts by preserving them before the fast-forward and
restoring them afterward. The target tracked source status after deploy was
clean.

After deployment, the production seed was run from `/opt`:

```bash
cd /opt/aps_database/APS_Database/data_processing_scripts
/opt/aps_database/venv/bin/python seed_irradiation_campaigns.py
```

Important seed result:

```text
Rows by action: insert=0, fill_blank=3, conflict=0,
accepted_conflict=0, code_owned_update=0, noop=17
```

The three `fill_blank` actions were the expected proton metadata updates:

| Campaign | Energy | Action |
| --- | ---: | --- |
| `Padova_Proton` | `1.0` MeV | Filled `let_surface`; updated notes. |
| `Padova_Proton` | `3.0` MeV | Filled `let_surface`; updated notes. |
| `PSI_Proton_2022` | `200.0` MeV | Filled `let_surface` and `range_um`; updated notes. |

Then the proxy readiness SQL/materialized views were rebuilt:

```bash
cd /opt/aps_database/APS_Database/data_processing_scripts
/opt/aps_database/venv/bin/python create_proxy_readiness_dashboard.py --schema-only
```

Result:

```text
Rebuilding proxy-readiness SQL views...
Proxy-readiness SQL views rebuilt
```

## Live Database Validation

Validation was run against the live production database
`postgresql://postgres@localhost:5435/mosfets`.

Proton LET and range metadata are now populated:

| Campaign | Ion | Energy MeV | `let_surface` | `range_um` |
| --- | --- | ---: | ---: | ---: |
| `Padova_Proton` | `proton` | `1` | `0.19153573628621` | `7` |
| `Padova_Proton` | `proton` | `3` | `0.0903069928796778` | `57` |
| `PSI_Proton_2022` | `proton` | `200` | `0.00374262719938149` | `97789.7465666261` |

The old chart bin is gone:

```sql
SELECT DISTINCT let_bin
FROM stress_test_context_view
WHERE let_bin LIKE 'LET proton%';
```

Result: `0` rows.

The PSI proton detected single-event rows are still present and chartable:

```sql
SELECT count(*)
FROM stress_test_context_view
WHERE source = 'irradiation'
  AND event_record_type = 'detected_single_event'
  AND radiation_deposited_energy_j > 0.0
  AND ion_species ILIKE 'proton%';
```

Result: `44`.

No seed metadata conflicts were recorded for rollout ref `96a1ac8`:

```sql
SELECT mode, count(*)
FROM seed_metadata_conflicts
WHERE git_ref = '96a1ac8'
GROUP BY mode
ORDER BY mode;
```

Result: `0` rows.

## Test Evidence

Final local checks:

```bash
python3 -m unittest discover -s tests
bash -n scripts/nightly_update_and_ingest.sh scripts/deploy_to_opt.sh
```

Results:

```text
Ran 35 tests in 0.058s
OK
```

Shell syntax checks passed.

The seed-specific test coverage locks the behavior that prevents recurrence:

1. Missing run produces `insert`.
2. `NULL` DB protected numeric plus seed value produces `fill_blank`.
3. Equal or tolerance-close numerics produce `noop`.
4. Different non-null protected numerics produce `conflict`.
5. `--accept-seed-conflicts` applies the seed numeric intentionally.
6. Seed `NULL` does not clear a curated non-null DB numeric.
7. `beam_type` and `notes` remain code-owned updates.
8. `ion_species` and `beam_energy_mev` are not update actions.
9. Legacy cleanup can suppress one-pass spurious conflicts for affected runs.

## Review Verdict

Review verdict: complete and correct, verified against the live DB.

This is a faithful implementation of the plan, including the parts that were
easy to get wrong. The verified code paths and production database state hold.

Confirmed correct:

1. Field policy is exact: protected numerics are
   `{let_surface, let_bragg_peak, range_um}`, code-owned fields are
   `{beam_type, notes}`, and identity fields are in neither set. Existing row
   identity is therefore not overwritten by upsert.
2. Nightly does not abort on default-mode conflicts. Default mode records
   conflicts, preserves DB values, and returns `0`; only `--strict` exits
   nonzero.
3. Nightly preflight is source-scoped and non-fatal. It checks only
   `data_processing_scripts`, `schema`, `scripts`, and `superset` with
   `--untracked-files=no`, so regenerated `out/` artifacts are ignored.
4. `deploy_to_opt.sh` handles the one-time dirty generated artifact migration
   from tracked to untracked. The `preserve_dirty_generated_artifacts()` helper
   plus the `EXIT` trap preserve local generated files around the fast-forward
   that removes them from the index.
5. Legacy cleanup interaction is handled by returning affected run ids from
   `cleanup_legacy_runs()` and passing them into conflict suppression for the
   same seed pass.
6. Live DB validation matches the intended final state: proton LETs populated,
   zero `LET proton` bins, `44` PSI proton event rows intact, and zero conflict
   records for the rollout.

## Notes

The working tree still had unrelated pre-existing dashboard and phase-doc
changes after the rollout. Those changes were intentionally left unstaged and
undeployed. The production deploy was performed from a clean temporary worktree
at commit `96a1ac8` so the `/opt` fast-forward used only the committed rollout
scope.
