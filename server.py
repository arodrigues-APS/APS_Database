##########################################################
##					APS Database Server     			##
##														##
## Authors: dorfer@aps.ethz.ch, 						##
## Date: November 3, 2022								##
## Version: 1											##
##########################################################
#python -m flask --debug --app server run (--host=0.0.0.0)

import os
from pathlib import Path
from configobj import ConfigObj
from flask import Flask, flash, jsonify, request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from flask import render_template, redirect, send_from_directory
from flaskext.markdown import Markdown
import psycopg2
import psycopg2.extras


from data_scraping import DataScraping


#load configuration file
conf = ConfigObj('config.ini')

#get DataScraping class object
ds = DataScraping(conf)
ds.search_results(sterms=[])  # pre-warm the file listing cache at startup

#simple class to hold all configuration parameters for the flask server
class Config():
	EXPLAIN_TEMPLATE_LOADING = True
	SECRET_KEY = os.environ.get('SECRET_KEY') or 'vRbPDgZP6rHpjCSQWByy'

app = Flask(__name__)
Markdown(app)
app.config.from_object(Config)

#class for defining search form
class SearchForm(FlaskForm):
	search_terms = StringField("search_terms")
	submit = SubmitField("Submit")


#define the URLs routes for the Website
@app.route("/search",methods =['POST','GET'])
def index():
	form = SearchForm()
	if form.validate_on_submit():
		sstring = form.search_terms.data
		if sstring == '':
			dirs = ds.search_results(sterms=[])
			dlink = f"rsync -a --progress <your_username>@{conf['Server']['url']}:/{conf['Data']['data_dir']} ."
		else:
			sterms = sstring.split(',')
			sterms = [s.strip() for s in sterms]
			#print(sterms)
			dirs = ds.search_results(sterms=sterms)

			#generate download link:
			dlink = f"rsync -a --progress <your_username>@{conf['Server']['url']}"
			for d in dirs:
				dlink += f":{d} "
			dlink += " ."
	else:
		dirs = ds.search_results(sterms=[])
		dlink = f"rsync -a --progress <your_username>@{conf['Server']['url']}:/{conf['Data']['data_dir']} ."

	return render_template("index.html", form=form, dirs=dirs, dlink=dlink)
	#return render_template("index.html", form=form)

@app.route("/data/www/<path:path>")
def send_file(path):
	return send_from_directory("/data/www", path)

@app.route("/readme")
def readme():
	md_text = Path('Readme.md').read_text()
	return render_template("readme.html", md_text=md_text)


# ── Device Library ───────────────────────────────────────────────────────────
from data_processing_scripts.db_config import get_connection
from data_processing_scripts.common import apply_schema

def get_db():
	"""Return a psycopg2 connection to the mosfets database."""
	return get_connection()

# Apply idempotent schema migrations (schema/*.sql).
# This must succeed at boot so admin pages cannot come up against a partial
# schema (which leads to confusing runtime errors).
try:
	_boot_conn = get_connection()
	apply_schema(_boot_conn)
	_boot_conn.close()
except Exception as _e:
	raise RuntimeError(f"Database schema bootstrap failed: {_e}") from _e

# Allowed values for drop-down fields
DEVICE_CATEGORIES = ["MOSFET", "Diode", "IGBT", "Other"]
MANUFACTURERS = ["Wolfspeed", "Infineon", "Rohm", "STMicroelectronics",
                 "onsemi", "Hitachi", "Mitsubishi", "Other"]
PACKAGE_TYPES = ["bare_die", "TO-247", "TO-263", "home_made_TO", "QFN", "Other"]

ION_SPECIES = ["proton", "neutron", "Au", "Ar", "Fe", "Xe", "Kr", "Ca",
               "C", "Cl", "N", "Ne", "Ni", "Si", "Other"]
BEAM_TYPES = ["broad_beam", "micro_beam", "Other"]
IRRAD_ROLES = ["pre_irrad", "post_irrad"]

MAPPING_PATTERN_TYPES = ["substring", "regex"]
MAPPING_SCOPES = ["all", "baselines", "sc", "irradiation", "avalanche"]

class DeviceForm(FlaskForm):
	part_number = StringField("Part Number")
	device_category = SelectField("Category", choices=DEVICE_CATEGORIES)
	manufacturer = SelectField("Manufacturer", choices=MANUFACTURERS)
	voltage_rating = StringField("Voltage Rating")
	rdson_mohm = StringField("RDS(on) mΩ")
	current_rating_a = StringField("Current Rating (A)")
	package_type = SelectField("Package", choices=PACKAGE_TYPES)
	notes = StringField("Notes")
	submit = SubmitField("Add Device")


class DeviceMappingRuleForm(FlaskForm):
	# part_number choices are populated at render time from device_library
	pattern = StringField("Pattern")
	pattern_type = SelectField("Type", choices=MAPPING_PATTERN_TYPES)
	scope = SelectField("Scope", choices=MAPPING_SCOPES)
	priority = StringField("Priority")
	part_number = SelectField("Device", choices=[])
	source_reference = StringField("Source")
	notes = StringField("Notes")
	submit = SubmitField("Add Rule")


IRRADIATION_ROOT = os.path.join(
	os.environ.get("APS_DATA_ROOT", "/home/arodrigues/APS_Database"),
	"Measurements", "Irradiation")

def list_irradiation_folders():
	"""Return folder names under Measurements/Irradiation/."""
	if not os.path.isdir(IRRADIATION_ROOT):
		return []
	return sorted(
		d for d in os.listdir(IRRADIATION_ROOT)
		if os.path.isdir(os.path.join(IRRADIATION_ROOT, d))
		and not d.startswith('.')
	)


def normalize_campaign_name(value):
	"""Normalize campaign names for stable uniqueness checks."""
	if value is None:
		return None
	value = " ".join(value.split())
	return value or None


def normalize_optional_text(value):
	"""Trim optional free-text fields and collapse empty strings to NULL."""
	if value is None:
		return None
	value = value.strip()
	return value or None

class CampaignForm(FlaskForm):
	campaign_name = StringField("Campaign Name")
	folder_name = SelectField("Data Folder", choices=[])
	facility = StringField("Facility")
	beam_type = SelectField("Beam Type", choices=BEAM_TYPES)
	notes = StringField("Notes")
	submit = SubmitField("Add Campaign")


@app.route("/", methods=["GET", "POST"])
@app.route("/devices", methods=["GET", "POST"])
def device_library():
	form = DeviceForm()
	error = None
	if form.validate_on_submit():
		pn = form.part_number.data.strip()
		if not pn:
			error = "Part number is required."
		else:
			try:
				conn = get_db()
				cur = conn.cursor()
				cur.execute(
					"""INSERT INTO device_library
					   (part_number, device_category, manufacturer,
					    voltage_rating, rdson_mohm, current_rating_a,
					    package_type, notes)
					   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
					(pn,
					 form.device_category.data,
					 form.manufacturer.data,
					 form.voltage_rating.data or None,
					 form.rdson_mohm.data or None,
					 form.current_rating_a.data or None,
					 form.package_type.data,
					 form.notes.data or None))
				conn.commit()
				cur.close()
				conn.close()
				flash(f"Device '{pn}' added.", "success")
				return redirect("/devices")
			except psycopg2.errors.UniqueViolation:
				error = f"Device '{pn}' already exists."
			except Exception as e:
				error = f"Database error: {e}"

	# Fetch current devices + mapping rules
	devices = []
	mapping_rules = []
	try:
		conn = get_db()
		cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute("SELECT * FROM device_library ORDER BY manufacturer, part_number")
		devices = cur.fetchall()
		cur.execute("""
			SELECT * FROM device_mapping_rules
			ORDER BY scope, priority DESC, LENGTH(pattern) DESC, pattern
		""")
		mapping_rules = cur.fetchall()
		cur.close()
		conn.close()
	except Exception as e:
		error = f"Could not load devices: {e}"

	rule_form = DeviceMappingRuleForm()
	rule_form.part_number.choices = [(d["part_number"], d["part_number"])
	                                 for d in devices]

	return render_template("devices.html",
	                       form=form, devices=devices,
	                       mapping_rules=mapping_rules,
	                       rule_form=rule_form,
	                       mapping_pattern_types=MAPPING_PATTERN_TYPES,
	                       mapping_scopes=MAPPING_SCOPES,
	                       error=error)


# ── Device Mapping Rules ─────────────────────────────────────────────────────

def _parse_priority(raw, default=100):
	try:
		return int(raw)
	except (TypeError, ValueError):
		return default


@app.route("/devices/mapping-rules", methods=["POST"])
def add_mapping_rule():
	pattern = request.form.get("pattern", "").strip()
	pattern_type = request.form.get("pattern_type", "substring")
	scope = request.form.get("scope", "all")
	priority = _parse_priority(request.form.get("priority"), 100)
	part_number = request.form.get("part_number", "").strip()
	if not pattern or not part_number:
		flash("Pattern and device are required.", "danger")
		return redirect("/devices")
	if pattern_type not in MAPPING_PATTERN_TYPES or scope not in MAPPING_SCOPES:
		flash("Invalid pattern type or scope.", "danger")
		return redirect("/devices")
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			INSERT INTO device_mapping_rules
			    (pattern, pattern_type, scope, priority, part_number,
			     source_reference, notes)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""",
			(pattern, pattern_type, scope, priority, part_number,
			 request.form.get("source_reference") or None,
			 request.form.get("notes") or None))
		conn.commit()
		cur.close(); conn.close()
		flash(f"Rule '{pattern}' → {part_number} added.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("A rule with that pattern/type/scope already exists.", "danger")
	except psycopg2.errors.ForeignKeyViolation:
		flash(f"Device '{part_number}' is not in the device library.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/devices")


@app.route("/devices/mapping-rules/edit/<int:rule_id>", methods=["POST"])
def edit_mapping_rule(rule_id):
	pattern_type = request.form.get("pattern_type", "substring")
	scope = request.form.get("scope", "all")
	if pattern_type not in MAPPING_PATTERN_TYPES or scope not in MAPPING_SCOPES:
		flash("Invalid pattern type or scope.", "danger")
		return redirect("/devices")
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE device_mapping_rules
			SET pattern          = %s,
			    pattern_type     = %s,
			    scope            = %s,
			    priority         = %s,
			    part_number      = %s,
			    source_reference = %s,
			    notes            = %s
			WHERE id = %s""",
			(request.form["pattern"].strip(),
			 pattern_type, scope,
			 _parse_priority(request.form.get("priority"), 100),
			 request.form["part_number"].strip(),
			 request.form.get("source_reference") or None,
			 request.form.get("notes") or None,
			 rule_id))
		conn.commit()
		cur.close(); conn.close()
		flash("Rule updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("Another rule with that pattern/type/scope already exists.", "danger")
	except psycopg2.errors.ForeignKeyViolation:
		flash("Referenced device is not in the device library.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/devices")


@app.route("/devices/mapping-rules/delete/<int:rule_id>", methods=["POST"])
def delete_mapping_rule(rule_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM device_mapping_rules WHERE id = %s", (rule_id,))
		conn.commit()
		cur.close(); conn.close()
		flash("Rule removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/devices")


@app.route("/devices/delete/<int:device_id>", methods=["POST"])
def delete_device(device_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM device_library WHERE id = %s", (device_id,))
		conn.commit()
		cur.close()
		conn.close()
		flash("Device removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/devices")


@app.route("/devices/edit/<int:device_id>", methods=["POST"])
def edit_device(device_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute(
			"""UPDATE device_library
			   SET part_number = %s,
			       device_category = %s,
			       manufacturer = %s,
			       voltage_rating = %s,
			       rdson_mohm = %s,
			       current_rating_a = %s,
			       package_type = %s,
			       notes = %s
			   WHERE id = %s""",
			(request.form["part_number"].strip(),
			 request.form["device_category"],
			 request.form["manufacturer"],
			 request.form["voltage_rating"] or None,
			 request.form["rdson_mohm"] or None,
			 request.form["current_rating_a"] or None,
			 request.form["package_type"],
			 request.form["notes"] or None,
			 device_id))
		conn.commit()
		cur.close()
		conn.close()
		flash("Device updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("A device with that part number already exists.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/devices")


# ── Irradiation Campaigns ─────────────────────────────────────────────────────

@app.route("/irradiation", methods=["GET", "POST"])
def irradiation():
	form = CampaignForm()
	folders = list_irradiation_folders()
	form.folder_name.choices = [("", "— none —")] + [(f, f) for f in folders]
	error = None
	if form.validate_on_submit():
		name = normalize_campaign_name(form.campaign_name.data)
		if not name:
			error = "Campaign name is required."
		else:
			try:
				conn = get_db()
				cur = conn.cursor()
				cur.execute("""
					INSERT INTO irradiation_campaigns
					    (campaign_name, folder_name, beam_type,
					     facility, notes)
					VALUES (%s, %s, %s, %s, %s)""",
					(name,
					 normalize_optional_text(form.folder_name.data),
					 normalize_optional_text(form.beam_type.data),
					 normalize_optional_text(form.facility.data),
					 normalize_optional_text(form.notes.data)))
				conn.commit()
				cur.close(); conn.close()
				flash(f"Campaign '{name}' added.", "success")
				return redirect("/irradiation")
			except psycopg2.errors.UniqueViolation:
				error = f"Campaign '{name}' already exists."
			except Exception as e:
				error = f"Database error: {e}"

	campaigns = []
	runs = []
	mappings = []
	experiments = []
	try:
		conn = get_db()
		cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute("SELECT * FROM irradiation_campaigns ORDER BY campaign_name")
		campaigns = cur.fetchall()
		cur.execute("""
			SELECT ir.*, ic.campaign_name
			FROM irradiation_runs ir
			JOIN irradiation_campaigns ic ON ir.campaign_id = ic.id
			ORDER BY ic.campaign_name, ir.ion_species, ir.beam_energy_mev
		""")
		runs = cur.fetchall()
		cur.execute("""
			SELECT ecm.*, ic.campaign_name
			FROM experiment_campaign_map ecm
			JOIN irradiation_campaigns ic ON ecm.campaign_id = ic.id
			ORDER BY ecm.experiment
		""")
		mappings = cur.fetchall()
		cur.execute("""
			SELECT DISTINCT experiment FROM baselines_metadata
			WHERE experiment IS NOT NULL ORDER BY experiment
		""")
		experiments = [r["experiment"] for r in cur.fetchall()]
		cur.close(); conn.close()
	except Exception as e:
		error = f"Could not load data: {e}"

	return render_template("irradiation.html",
	                       form=form, campaigns=campaigns,
	                       runs=runs,
	                       mappings=mappings, experiments=experiments,
	                       folders=folders,
	                       ion_species=ION_SPECIES, beam_types=BEAM_TYPES,
	                       irrad_roles=IRRAD_ROLES, error=error)


@app.route("/irradiation/delete/<int:campaign_id>", methods=["POST"])
def delete_campaign(campaign_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM irradiation_campaigns WHERE id = %s", (campaign_id,))
		conn.commit()
		cur.close(); conn.close()
		flash("Campaign removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


@app.route("/irradiation/edit/<int:campaign_id>", methods=["POST"])
def edit_campaign(campaign_id):
	name = normalize_campaign_name(request.form.get("campaign_name"))
	if not name:
		flash("Campaign name is required.", "danger")
		return redirect("/irradiation")
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE irradiation_campaigns
			SET campaign_name   = %s,
			    folder_name     = %s,
			    beam_type       = %s,
			    facility        = %s,
			    notes           = %s
			WHERE id = %s""",
			(name,
			 normalize_optional_text(request.form.get("folder_name")),
			 normalize_optional_text(request.form.get("beam_type")),
			 normalize_optional_text(request.form.get("facility")),
			 normalize_optional_text(request.form.get("notes")),
			 campaign_id))
		conn.commit()
		cur.close(); conn.close()
		flash("Campaign updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("A campaign with that name already exists.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


# ── Irradiation Runs ─────────────────────────────────────────────────────────

@app.route("/irradiation/run", methods=["POST"])
def add_irradiation_run():
	campaign_id = request.form.get("campaign_id", "").strip()
	ion_species = request.form.get("ion_species", "").strip()
	if not campaign_id or not ion_species:
		flash("Campaign and ion species are required.", "danger")
		return redirect("/irradiation")
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			INSERT INTO irradiation_runs
			    (campaign_id, ion_species, beam_energy_mev,
			     let_surface, let_bragg_peak, range_um,
			     beam_type, notes)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
			(int(campaign_id), ion_species,
			 request.form.get("beam_energy_mev") or None,
			 request.form.get("let_surface") or None,
			 request.form.get("let_bragg_peak") or None,
			 request.form.get("range_um") or None,
			 request.form.get("run_beam_type") or None,
			 request.form.get("run_notes") or None))
		conn.commit()
		cur.close(); conn.close()
		flash(f"Run '{ion_species}' added to campaign.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("That ion/energy combination already exists for this campaign.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


@app.route("/irradiation/run/edit/<int:run_id>", methods=["POST"])
def edit_irradiation_run(run_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE irradiation_runs
			SET ion_species     = %s,
			    beam_energy_mev = %s,
			    let_surface     = %s,
			    let_bragg_peak  = %s,
			    range_um        = %s,
			    beam_type       = %s,
			    notes           = %s
			WHERE id = %s""",
			(request.form["ion_species"],
			 request.form.get("beam_energy_mev") or None,
			 request.form.get("let_surface") or None,
			 request.form.get("let_bragg_peak") or None,
			 request.form.get("range_um") or None,
			 request.form.get("run_beam_type") or None,
			 request.form.get("run_notes") or None,
			 run_id))
		conn.commit()
		cur.close(); conn.close()
		flash("Run updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("That ion/energy combination already exists for this campaign.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


@app.route("/irradiation/run/delete/<int:run_id>", methods=["POST"])
def delete_irradiation_run(run_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM irradiation_runs WHERE id = %s", (run_id,))
		conn.commit()
		cur.close(); conn.close()
		flash("Run removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


# ── Experiment Mappings ──────────────────────────────────────────────────────

@app.route("/irradiation/map", methods=["POST"])
def add_experiment_mapping():
	experiment = request.form.get("experiment", "").strip()
	campaign_id = request.form.get("campaign_id", "").strip()
	role = request.form.get("role", "post_irrad")
	if not experiment or not campaign_id:
		flash("Experiment and campaign are required.", "danger")
		return redirect("/irradiation")
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			INSERT INTO experiment_campaign_map (experiment, campaign_id, role)
			VALUES (%s, %s, %s)
			ON CONFLICT (experiment)
			DO UPDATE SET campaign_id = EXCLUDED.campaign_id,
			              role        = EXCLUDED.role""",
			(experiment, int(campaign_id), role))
		conn.commit()
		cur.close(); conn.close()
		flash(f"Mapped '{experiment}' to campaign.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


@app.route("/irradiation/map/delete/<int:map_id>", methods=["POST"])
def delete_experiment_mapping(map_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM experiment_campaign_map WHERE id = %s", (map_id,))
		conn.commit()
		cur.close(); conn.close()
		flash("Mapping removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


# ── Assign Run to Measurements ───────────────────────────────────────────────

@app.route("/irradiation/assign_run", methods=["POST"])
def assign_run_to_measurements():
	"""Bulk-assign an irradiation run to all measurements matching filters."""
	run_id = request.form.get("run_id", "").strip()
	campaign_id = request.form.get("filter_campaign_id", "").strip()
	device_filter = request.form.get("filter_device_id", "").strip()
	filename_pattern = request.form.get("filter_filename", "").strip()
	if not run_id or not campaign_id:
		flash("Run and campaign are required.", "danger")
		return redirect("/irradiation")
	try:
		conn = get_db()
		cur = conn.cursor()
		rid = int(run_id)
		cid = int(campaign_id)
		cur.execute("SELECT campaign_id FROM irradiation_runs WHERE id = %s",
		            (rid,))
		run_row = cur.fetchone()
		if not run_row:
			flash("Selected irradiation run does not exist.", "danger")
			cur.close(); conn.close()
			return redirect("/irradiation")
		if run_row[0] != cid:
			flash("Selected run does not belong to the selected campaign.", "danger")
			cur.close(); conn.close()
			return redirect("/irradiation")
		# Build WHERE clause for filtering measurements
		conditions = ["md.irrad_campaign_id = %s"]
		params = [cid]
		if device_filter:
			conditions.append("md.device_id ILIKE %s")
			params.append(f"%{device_filter}%")
		if filename_pattern:
			conditions.append("md.filename ILIKE %s")
			params.append(f"%{filename_pattern}%")
		where = " AND ".join(conditions)
		sql = f"""
			UPDATE baselines_metadata md
			SET irrad_run_id = %s
			WHERE {where}
			  AND (md.irrad_run_id IS DISTINCT FROM %s)
		"""
		cur.execute(sql, (rid, *params, rid))
		n = cur.rowcount
		conn.commit()
		cur.close(); conn.close()
		flash(f"Assigned run to {n} measurement(s).", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


@app.route("/irradiation/sync", methods=["POST"])
def sync_irradiation_metadata():
	"""Propagate experiment_campaign_map assignments to baselines_metadata."""
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE baselines_metadata md
			SET irrad_campaign_id = ecm.campaign_id,
			    irrad_role        = ecm.role
			FROM experiment_campaign_map ecm
			WHERE md.experiment = ecm.experiment
			  AND (md.irrad_campaign_id IS DISTINCT FROM ecm.campaign_id
			       OR md.irrad_role IS DISTINCT FROM ecm.role)
		""")
		n = cur.rowcount
		conn.commit()
		cur.close(); conn.close()
		flash(f"Sync complete: {n} metadata row(s) updated.", "success")
	except Exception as e:
		flash(f"Sync error: {e}", "danger")
	return redirect("/irradiation")


# ── Avalanche Campaigns ───────────────────────────────────────────────────────

from data_processing_scripts.db_config import NAS_ROOT

AVALANCHE_ROOT = os.path.join(NAS_ROOT, "Avalanche Measurements")

AVALANCHE_OUTCOMES = ["unknown", "survived", "failed"]


class AvalancheCampaignForm(FlaskForm):
	folder_path        = StringField("Folder Path")
	campaign_name      = StringField("Campaign Name")
	inductance_mh      = StringField("Inductance (mH)")
	temperature_c      = StringField("Temperature (°C)")
	device_part_number = StringField("Device Part Number")
	outcome_default    = SelectField("Default Outcome", choices=AVALANCHE_OUTCOMES)
	notes              = StringField("Notes")
	submit             = SubmitField("Add Campaign")


_ENSURE_AVALANCHE_SQL = """
CREATE TABLE IF NOT EXISTS avalanche_campaigns (
    id                 SERIAL PRIMARY KEY,
    folder_path        TEXT NOT NULL UNIQUE,
    campaign_name      TEXT NOT NULL,
    inductance_mh      DOUBLE PRECISION,
    temperature_c      DOUBLE PRECISION,
    device_part_number TEXT,
    outcome_default    TEXT DEFAULT 'unknown',
    notes              TEXT
);
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN avalanche_inductance_mh DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
"""


def _ensure_avalanche_schema(conn):
	cur = conn.cursor()
	cur.execute(_ENSURE_AVALANCHE_SQL)
	conn.commit()
	cur.close()


def _list_avalanche_families(conn):
	"""Return distinct avalanche_family values already in baselines_metadata."""
	try:
		cur = conn.cursor()
		cur.execute("""
			SELECT DISTINCT avalanche_family
			FROM baselines_metadata
			WHERE data_source = 'avalanche' AND avalanche_family IS NOT NULL
			ORDER BY avalanche_family
		""")
		rows = [r[0] for r in cur.fetchall()]
		cur.close()
		return rows
	except Exception:
		return []


@app.route("/avalanche", methods=["GET", "POST"])
def avalanche():
	error = None
	conn = get_db()
	try:
		_ensure_avalanche_schema(conn)
	except Exception as e:
		error = f"Schema error: {e}"

	form = AvalancheCampaignForm()
	if form.validate_on_submit():
		fp = form.folder_path.data.strip()
		name = form.campaign_name.data.strip()
		if not fp or not name:
			error = "Folder path and campaign name are required."
		else:
			try:
				cur = conn.cursor()
				cur.execute("""
					INSERT INTO avalanche_campaigns
					    (folder_path, campaign_name, inductance_mh,
					     temperature_c, device_part_number,
					     outcome_default, notes)
					VALUES (%s, %s, %s, %s, %s, %s, %s)""",
					(fp, name,
					 form.inductance_mh.data or None,
					 form.temperature_c.data or None,
					 form.device_part_number.data.strip() or None,
					 form.outcome_default.data or "unknown",
					 form.notes.data or None))
				conn.commit()
				cur.close()
				flash(f"Campaign '{name}' added.", "success")
				return redirect("/avalanche")
			except psycopg2.errors.UniqueViolation:
				conn.rollback()
				error = f"A campaign for folder '{fp}' already exists."
			except Exception as e:
				conn.rollback()
				error = f"Database error: {e}"

	campaigns = []
	known_families = []
	try:
		cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute("SELECT * FROM avalanche_campaigns ORDER BY folder_path")
		campaigns = cur.fetchall()
		cur.close()
		known_families = _list_avalanche_families(conn)
	except Exception as e:
		error = f"Could not load campaigns: {e}"
	finally:
		conn.close()

	return render_template("avalanche.html",
	                       form=form, campaigns=campaigns,
	                       known_families=known_families,
	                       outcomes=AVALANCHE_OUTCOMES,
	                       error=error)


@app.route("/avalanche/delete/<int:campaign_id>", methods=["POST"])
def delete_avalanche_campaign(campaign_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("DELETE FROM avalanche_campaigns WHERE id = %s", (campaign_id,))
		conn.commit()
		cur.close(); conn.close()
		flash("Campaign removed.", "success")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/avalanche")


@app.route("/avalanche/edit/<int:campaign_id>", methods=["POST"])
def edit_avalanche_campaign(campaign_id):
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE avalanche_campaigns
			SET folder_path        = %s,
			    campaign_name      = %s,
			    inductance_mh      = %s,
			    temperature_c      = %s,
			    device_part_number = %s,
			    outcome_default    = %s,
			    notes              = %s
			WHERE id = %s""",
			(request.form["folder_path"].strip(),
			 request.form["campaign_name"].strip(),
			 request.form.get("inductance_mh") or None,
			 request.form.get("temperature_c") or None,
			 request.form.get("device_part_number", "").strip() or None,
			 request.form.get("outcome_default") or "unknown",
			 request.form.get("notes") or None,
			 campaign_id))
		conn.commit()
		cur.close(); conn.close()
		flash("Campaign updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("A campaign for that folder path already exists.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/avalanche")


@app.route("/avalanche/sync", methods=["POST"])
def sync_avalanche_metadata():
	"""Push inductance / temperature / device overrides to baselines_metadata null fields."""
	try:
		conn = get_db()
		_ensure_avalanche_schema(conn)
		cur = conn.cursor()

		cur.execute("""
			UPDATE baselines_metadata bm
			SET avalanche_inductance_mh = ac.inductance_mh
			FROM avalanche_campaigns ac
			WHERE bm.data_source = 'avalanche'
			  AND bm.avalanche_family = ac.folder_path
			  AND ac.inductance_mh IS NOT NULL
			  AND bm.avalanche_inductance_mh IS NULL
		""")
		n_ind = cur.rowcount

		cur.execute("""
			UPDATE baselines_metadata bm
			SET avalanche_temperature_c = ac.temperature_c
			FROM avalanche_campaigns ac
			WHERE bm.data_source = 'avalanche'
			  AND bm.avalanche_family = ac.folder_path
			  AND ac.temperature_c IS NOT NULL
			  AND bm.avalanche_temperature_c IS NULL
		""")
		n_temp = cur.rowcount

		cur.execute("""
			UPDATE baselines_metadata bm
			SET device_type = ac.device_part_number
			FROM avalanche_campaigns ac
			WHERE bm.data_source = 'avalanche'
			  AND bm.avalanche_family = ac.folder_path
			  AND ac.device_part_number IS NOT NULL
			  AND bm.device_type IS NULL
		""")
		n_dev = cur.rowcount

		conn.commit()
		cur.close(); conn.close()
		flash(
			f"Sync complete: {n_ind} inductance, {n_temp} temperature, "
			f"{n_dev} device-type field(s) updated.",
			"success"
		)
	except Exception as e:
		flash(f"Sync error: {e}", "danger")
	return redirect("/avalanche")


if __name__ == "__main__":
	app.run(debug=False)
