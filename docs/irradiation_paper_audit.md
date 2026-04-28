# Irradiation Paper Audit

Reviewed on 2026-04-28 against PDFs under
`/home/arodrigues/APS_Database/relevant papers`.

Follow-up web check on 2026-04-28 found a PSI-specific open-access paper
that is not present in the local paper folder.

## Local Campaign Metadata Added To Seeds

These items are represented in `data_processing_scripts/seed_irradiation_campaigns.py`.

- UCL / RADEF heavy-ion broad beam: corrected to Fe, Ni, Kr, and Xe total kinetic energies, LET, and range from the Martinella 2024 trench-gate paper. The older N/Ne rows were stale.
- UCL campaign notes: added the Ba thesis broad-beam frame context for Ni/Kr/Xe and the Xe fluxes reported at 350 V.
- ANSTO / GSI microbeam: corrected C, Cl, Ni, Ar, and Au energies, LET, and ranges from the Martinella 2025 microbeam paper.
- GSI March 2025: represented as a microbeam campaign; Au 1162 MeV is the local folder run. Ar 344 MeV remains as a literature-supported run but should be assigned only if a matching local folder/logbook is confirmed.
- GSI Ca 2022: represented as a Ca 344 MeV microbeam run with thesis scan-frame and ion-count context.
- Padova proton: represented as 1 MeV and 3 MeV proton runs at the INFN-LNL CN accelerator. The dashboard ranges use the MOSFET ranges after top metallization, with wafer/DLTS ranges retained in notes:
  - 1 MeV: MOSFET range about 7 um; wafer/DLTS range about 10.8 um.
  - 3 MeV: MOSFET range about 57 um; wafer/DLTS range about 62 um.
- PSI proton 2022: matched to Martinella et al. 2023, "High-Energy Proton and Atmospheric-Neutron Irradiations of SiC Power MOSFETs: SEB Study and Impact on Channel and Drift Resistances." The local logbook device references and voltage conditions match the paper's PSI/PIF 200 MeV proton campaign.

## Relevant But Not Seeded As IV Campaigns

- Padova 10 keV X-ray exposure is described in the trench TID/DD paper. The ingestion code currently skips the corresponding local folders because they are logbook/C-V style data rather than IV irradiation measurements. Add a separate campaign type before surfacing this in the Flask irradiation dashboard.
- The overview/device inventory PDF maps device families to campaigns and packages. It is useful for device-library annotations, but it does not add new beam/run rows.

## External Literature Campaigns

These papers contain valid irradiation campaign tables, but they describe external literature datasets rather than local APS measurement campaigns. They should not be mixed into `irradiation_runs` unless the app gains a separate literature-reference catalog.

- NASA / TAMU / LBNL SEE papers and poster: Ne, Ar, Ag, Xe, B, Cu, and Kr beams from TAMU/LBNL.
- SELC statistics papers: Texas A&M and HIRFL heavy-ion experiments, including Ta-181 at HIRFL.
- Radiation-FOM-like papers: UTTAC Al beam and other external SiC radiation studies.
- Avalanche and linking-stress papers: useful reliability background, but no local irradiation campaign rows.

## Remaining Gaps

- D2019 proton still has unknown proton energy in the available papers reviewed here and in follow-up title/keyword searches.
- The PSI paper is not currently stored under `/home/arodrigues/APS_Database/relevant papers`; add the PDF there if the folder should remain the complete local literature source.
- If X-ray/TID campaigns should appear alongside ion/proton campaigns, the schema needs a radiation-source field rather than overloading `ion_species`.
