#!/usr/bin/env python
"""
Uses rpm -qa and additional rpm -qi <package_name> to abstract name,version,release.
Pushes three different title types 'NEW,REMOVED,CHANGED'.
Used for rpm annotations querying on influxdb/grafana.
Does not used inherited publish command from diamond.collector.Collector
Sends published data via influxdb

Must have read/write access to specified save_file

### Dependencies
	python-influxdb,
	rpm installed on local system


"""
try:
	import subprocess
	import re
	import os
	from influxdb import client as influxdb
	import diamond.collector

except ImportError:
	influxdb = None

class DictDiffer(object):
	"""
	Calculate the difference between two dictionaries as:
	(1) items added
	(2) items removed
	(3) keys same in both but changed values
	(4) keys same in both and unchanged values
	"""
	def __init__(self, current_dict, past_dict):
		self.current_dict, self.past_dict = current_dict, past_dict
		self.current_keys, self.past_keys = [
			set(d.keys()) for d in (current_dict, past_dict)
		]
		self.intersect = self.current_keys.intersection(self.past_keys)

	def added(self):
		return self.current_keys - self.intersect

	def removed(self):
		return self.past_keys - self.intersect

	def changed(self):
		return set(o for o in self.intersect
			if self.past_dict[o] != self.current_dict[o])

	def unchanged(self):
		return set(o for o in self.intersect
			if self.past_dict[o] == self.current_dict[o])



class InfluxdbRpmAnnotationsCollector(diamond.collector.Collector):

	def get_default_config_help(self):
		config_help = super(RpmCollector, self).get_default_config_help()
		config_help.update({
			'rpm_binary_location' : "Location of rpm binary",
			'save_file': "Where to store previous run rpm information",
			'influxdb_host': "InfluxDB IP",
			'influxdb_port': "InfluxDB API port",
			'influxdb_user': "InfluxDB username for specified database",
			'influxdb_pass': "InfluxDB password for specified database",
			'influxdb_database': "InfluxDB specified database to send annotations to"
			})
		return config_help


	def get_default_config(self):
		"""
		Returns the deafult collector settings
		"""
		config = super(RpmCollector, self).get_default_config()
		config.update({
			'rpm_binary_location' : "/bin/rpm",
			'save_file': "/tmp/rpmvaluelist",
			'influxdb_host': "localhost",
			'influxdb_port': "8086",
			'influxdb_user': "root",
			'influxdb_pass': "root",
			'influxdb_database': "diamond"
			})
		return config

	def get_rpmvalues(self): #could possibly use rpm -qa --last
		
		rpm_dict = {}
		rpm_qa_command = [self.config['rpm_binary_location'],"-qa"]
		
		try:
			rpm_list = subprocess.Popen(rpm_qa_command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
		except ValueError as err:
			self.log.error("Invalid arguments provided to rpm query all")
		except OSError as err:
			self.log.error("OSError while perfoming rpm query all, may have invalid arguments or locations")


		stderr = rpm_list.stderr.readlines()

		if len(stderr) == 0:
			pass
		else:
			self.log.error("stderr reported from rpm query all")

		for rpm in rpm_list.stdout:
			rpm_stripped = rpm.rstrip()

			rpm_qi_command = [self.config['rpm_binary_location'],"-qi",rpm_stripped]

			try:
				rpm_info = subprocess.Popen(rpm_qi_command,stdout=subprocess.PIPE)
			except ValueError as err:
				self.log.error("Invalid arguments provided to rpm query info")
			except OSError as err:
				self.log.error("OSError while perfoming rpm query info, may have invalid arguments or locations")

			name_line = rpm_info.stdout.readline()
			version_line = rpm_info.stdout.readline()
			release_line = rpm_info.stdout.readline()

			name_re = re.match("(?:Name\s*\:\s)([\w\-]+)",name_line,re.I)
			version_re = re.match("(?:Version\s*\:\s)([0-9\.]+)",version_line,re.I)
			release_re = re.match("(?:Release\s*\:\s)([\w\.]+)",release_line,re.I)


			if (name_re and version_re and release_re):
				rpm_dict[name_re.group(1)] = "{0}-{1}".format(version_re.group(1),release_re.group(1))   #[version_re.group(1),release_re.group(1)]
			else:
				pass
		

		return rpm_dict	

	def create_rpmvalues(self,dict_already_created=None):

		if dict_already_created == None:
			rpm_dict = self.get_rpmvalues()
		else:
			rpm_dict = dict_already_created

		try:
			with open(self.config['save_file'],'w+') as fh:
				for package,version_and_release in rpm_dict.items():
					fh.write("{0},{1}\n".format(package,version_and_release))
		except EnvironmentError as err:
			self.log.error("EnvironmenttError while writing to - {0}".format(self.config['save_file']))


	def load_rpmvalues(self,):
		
		PACKAGE_NAME = 0
		VERSION_AND_RELEASE_INDEX = 1
		file_rpm_dict = {}
		try:
			with open(self.config['save_file'],'r') as fh:
				for line in fh:
					delimit = line.split(',')
					if len(delimit) != 2:
						self.log.warn("{0} in {1} is not valid".format(line,self.config['save_file']))
					else:
						delimit[VERSION_AND_RELEASE_INDEX] = delimit[VERSION_AND_RELEASE_INDEX].rstrip()
						file_rpm_dict[delimit[PACKAGE_NAME]] = delimit[VERSION_AND_RELEASE_INDEX]


			return file_rpm_dict

		except EnvironmentError as err:
			self.log.error("EnvironmenttError while loading - {0}".format(self.config['save_file']))




	def write_to_influxdb(self,influxdb_connection,rpm_name,rpm_annotation_type=None,rpm_value=None,former_value=None,new_value=None):
		
		series = "{0}".format(self.get_metric_path("rpm"))

		if rpm_annotation_type == "NEW":
			title = "NEW RPM"
			text = "{0}-{1}".format(rpm_name,rpm_value)
		elif rpm_annotation_type == "REMOVE":
			title = "REMOVED RPM"
			text = "{0}-{1}".format(rpm_name,rpm_value)
		elif rpm_annotation_type == "CHANGE":
			title = "RPM CHANGE"
			if former_value == None or new_value == None:
				self.log.error("CHANGE type require former_value and new_value")
			else:
				text = "FROM {0}-{1} TO {0}-{2}".format(rpm_name,former_value,new_value)

		data = [
				{
					"points": [[title,text]],
					"name": series,
					"columns": ["title","text"]
				}
			]

		try:
			influxdb_connection.write_points(data)		
		except:
			self.log.error("Unable to write points to specified influxdb host")


	def collect(self):

		if not influxdb:
			self.log.error("python-influxdb not installed")

		if not os.path.exists(self.config['save_file']):
			self.create_rpmvalues()
		else:
			current_rpm_dict = self.get_rpmvalues()
			prior_rpm_dict = self.load_rpmvalues()

			differ = DictDiffer(current_rpm_dict,prior_rpm_dict)

			try:
				db = influxdb.InfluxDBClient(self.config["influxdb_host"],self.config["influxdb_port"],self.config["influxdb_user"],self.config["influxdb_pass"],self.config["influxdb_database"])
			except:
				self.log.error("Invalid influxDB connection parameters or host is not available")

			new_rpms_set = differ.added()
			removed_rpms_set = differ.removed()
			upgraded_rpms_set = differ.changed()


			# Probably could zip the first two
			if new_rpms_set:
				for rpm in new_rpms_set:
					self.write_to_influxdb(db,rpm,rpm_annotation_type="NEW",rpm_value=current_rpm_dict[rpm])
			if removed_rpms_set:
				for rpm in removed_rpms_set:
					 self.write_to_influxdb(db,rpm,rpm_annotation_type="REMOVE",rpm_value=prior_rpm_dict[rpm])
			if upgraded_rpms_set:
				for rpm in upgraded_rpms_set:
					self.write_to_influxdb(db,rpm,rpm_annotation_type="CHANGE",former_value=prior_rpm_dict[rpm],new_value=current_rpm_dict[rpm])


			self.create_rpmvalues(current_rpm_dict)
