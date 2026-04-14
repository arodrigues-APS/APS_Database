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

def get_db():
	"""Return a psycopg2 connection to the mosfets database."""
	return get_connection()

# Allowed values for drop-down fields
DEVICE_CATEGORIES = ["MOSFET", "Diode", "IGBT", "Other"]
MANUFACTURERS = ["Wolfspeed", "Infineon", "Rohm", "STMicroelectronics",
                 "onsemi", "Hitachi", "Mitsubishi", "Other"]
PACKAGE_TYPES = ["bare_die", "TO-247", "TO-263", "home_made_TO", "QFN", "Other"]

ION_SPECIES = ["proton", "neutron", "Au", "Fe", "Xe", "Ca", "C", "Si", "Other"]
BEAM_TYPES = ["broad_beam", "micro_beam", "Other"]
IRRAD_ROLES = ["pre_irrad", "post_irrad"]

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


class CampaignForm(FlaskForm):
	campaign_name = StringField("Campaign Name")
	ion_species = SelectField("Ion Species", choices=ION_SPECIES)
	beam_energy_mev = StringField("Beam Energy (MeV)")
	beam_type = SelectField("Beam Type", choices=BEAM_TYPES)
	facility = StringField("Facility")
	fluence_range = StringField("Fluence Range")
	let_mev_cm2_mg = StringField("LET (MeV cm²/mg)")
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

	# Fetch current devices
	devices = []
	try:
		conn = get_db()
		cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute("SELECT * FROM device_library ORDER BY manufacturer, part_number")
		devices = cur.fetchall()
		cur.close()
		conn.close()
	except Exception as e:
		error = f"Could not load devices: {e}"

	return render_template("devices.html", form=form, devices=devices, error=error)


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
	error = None
	if form.validate_on_submit():
		name = form.campaign_name.data.strip()
		if not name:
			error = "Campaign name is required."
		else:
			try:
				conn = get_db()
				cur = conn.cursor()
				cur.execute("""
					INSERT INTO irradiation_campaigns
					    (campaign_name, ion_species, beam_energy_mev, beam_type,
					     facility, fluence_range, let_mev_cm2_mg, notes)
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
					(name,
					 form.ion_species.data,
					 form.beam_energy_mev.data or None,
					 form.beam_type.data or None,
					 form.facility.data or None,
					 form.fluence_range.data or None,
					 form.let_mev_cm2_mg.data or None,
					 form.notes.data or None))
				conn.commit()
				cur.close(); conn.close()
				flash(f"Campaign '{name}' added.", "success")
				return redirect("/irradiation")
			except psycopg2.errors.UniqueViolation:
				error = f"Campaign '{name}' already exists."
			except Exception as e:
				error = f"Database error: {e}"

	campaigns = []
	mappings = []
	experiments = []
	try:
		conn = get_db()
		cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute("SELECT * FROM irradiation_campaigns ORDER BY campaign_name")
		campaigns = cur.fetchall()
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
	                       mappings=mappings, experiments=experiments,
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
	try:
		conn = get_db()
		cur = conn.cursor()
		cur.execute("""
			UPDATE irradiation_campaigns
			SET campaign_name   = %s,
			    ion_species     = %s,
			    beam_energy_mev = %s,
			    beam_type       = %s,
			    facility        = %s,
			    fluence_range   = %s,
			    let_mev_cm2_mg  = %s,
			    notes           = %s
			WHERE id = %s""",
			(request.form["campaign_name"].strip(),
			 request.form["ion_species"],
			 request.form["beam_energy_mev"] or None,
			 request.form["beam_type"] or None,
			 request.form["facility"] or None,
			 request.form["fluence_range"] or None,
			 request.form["let_mev_cm2_mg"] or None,
			 request.form["notes"] or None,
			 campaign_id))
		conn.commit()
		cur.close(); conn.close()
		flash("Campaign updated.", "success")
	except psycopg2.errors.UniqueViolation:
		flash("A campaign with that name already exists.", "danger")
	except Exception as e:
		flash(f"Error: {e}", "danger")
	return redirect("/irradiation")


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


if __name__ == "__main__":
	app.run(debug=False)

