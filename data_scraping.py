##########################################################
##					APS Database Server     			##
##														##
## Authors: dorfer@aps.ethz.ch, 						##
## Date: November 3, 2022								##
## Version: 1											##
##########################################################


import os
import time
from pathlib import Path


class DataScraping():
	"""
	The DataScraping class scrapes the data folder defined in the passed config file (data_dir=..)
	and returns a list of files but also empty folders folders. Files in the config file's exclude
	keyword (exclude=..) are ignored.

	Attributes
	----------
	conf : ConfigObj
		A configuration file object. The library configobj2 was used in v1
	"""

	def __init__(self, conf):
		self.conf = conf
		self.ddir = self.conf['Data']['data_dir']
		self.excluded_files = self.conf['Data']['excluded']
		self._cache = None
		self._cache_time = 0
		self._cache_ttl = 180  # seconds (3 minutes)


	def shouldbe_listed_path(self, path):
		"""
		Based on the Path (pathlib) of a file/directory this function checks whether or not to display it on the website.
		Standard behavior: (empty folders + files) minus the files in self.excluded_files are displayed
		"""
		try:
			if path.name in self.excluded_files: #filename matches any pattern in excluded files?
				return False

			if os.path.isdir(path) and os.path.exists(path) and len(os.listdir(path)) > 0: #directory with content? - will be covered when files are listed
				return False

			if path.is_file():
				return True
		except Exception as e:
			print("Exception: ", e)
			pass

		return True


	def scandir(self):
		"""
		Returns a generator object of paths to be listed on the website.
		"""
		for p in Path(self.ddir).rglob("*"):
			if not self.shouldbe_listed_path(p):
				continue
			yield p


	def search_results(self, sterms):
		"""
		Filter the results returned by 'scandir' for keywords given in sterms.
		Only if all keywords are found in a path it is returned.
		
		Parameters
		----------
		sterms: list
			A list (can be empty) of search terms (str) that all should be included in the path.

		"""


		now = time.time()
		if self._cache is None or (now - self._cache_time) > self._cache_ttl:
			self._cache = [str(p) for p in self.scandir()]
			self._cache_time = now

		dirs = self._cache

		if len(sterms) == 0: #no search term - return all data
			return list(dirs)

		#make all search terms lowercase
		sterms = [s.lower() for s in sterms]

		#make all paths lowercase but keep the original also
		dirs = [[d.lower(), d] for d in dirs]
		

		
		#print(dirs)

		dirs = [dirname[1] for dirname in dirs if all(substring in dirname[0] for substring in sterms)] #finds all entries that includes ALL search terms
		#print('found: ', dirs)
		return dirs