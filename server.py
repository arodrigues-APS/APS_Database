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
@app.route("/",methods =['POST','GET'])
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
DB_CONFIG = dict(host="localhost", port=5435, dbname="mosfets",
                 user="postgres", password="APSLab")

def get_db():
	"""Return a psycopg2 connection to the mosfets database."""
	return psycopg2.connect(**DB_CONFIG)

# Allowed values for drop-down fields
DEVICE_CATEGORIES = ["MOSFET", "Diode", "IGBT", "Other"]
MANUFACTURERS = ["Wolfspeed", "Infineon", "Rohm", "STMicroelectronics",
                 "onsemi", "Hitachi", "Mitsubishi", "Other"]
PACKAGE_TYPES = ["bare_die", "TO-247", "TO-263", "home_made_TO", "QFN", "Other"]

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


if __name__ == "__main__":
	app.run(debug=False)

