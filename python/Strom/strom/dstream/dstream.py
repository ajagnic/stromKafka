"""Base class for dstream
This dictionary defines our basic data structure.
"""
import uuid

from strom.utils.logger.logger import logger

__version__  = "0.1"
__author__ = "David <david@tura.io>"


class DStream(dict):
    def __init__(self):
        """
        Initializes and empty DStream with a unique stream token. All the other expected keys are
        initialized as empty data structures of the desired type.
        """
        self["stream_name"] = None
        self["version"] = 0
        self["stream_token"] = uuid.uuid1()
        self["sources"] = {}
        self["storage_rules"] = {}
        self["ingest_rules"] = {}
        self["engine_rules"] = {}
        self["timestamp"] = None
        self["measures"] = {}
        self["fields"] = {}
        self["user_ids"] = {}
        self["tags"] = {}
        self["foreign_keys"] = {}
        self["filters"] = []
        self["dparam_rules"] = []
        self["event_rules"] = {}
        logger.debug("DStream initialize")


    def _add_source(self, source_name, source_location):
        """Adds source_location to our dict of sources under source_name
        source_location: dict with key(s):
            type: file or kafka
            if type == kafka, we also have
            topic: kafka topic name"""
        self["sources"][source_name] = source_location
        logger.debug("added source %s" % (source_name))

    def _add_measure(self, measure_name, dtype):
        """Creates entry in measures dict for new measure"""
        self["measures"][measure_name] = {"val":None, "dtype":dtype}
        logger.debug("added measure %s" % (measure_name))

    def _add_field(self, field_name):
        self["fields"][field_name] = {}
        logger.debug("added field %s" % (field_name))

    def _add_user_id(self, id_name):
        self["user_ids"][id_name] = {}
        logger.debug("added id_name %s" % (id_name))

    def _add_tag(self, tag_name):
        self["tags"][tag_name] = {}
        logger.debug("added tag %s" % (tag_name))

    def _add_fk(self, foreign_key):
        self["foreign_keys"][foreign_key] = {}
        logger.debug("added key %s" % (foreign_key))

    def _add_filter(self, filter_dict):
        """Add filter to our storage.
         filter_dict: dict of parameters for filter class object"""
        self["filters"].append(filter_dict)
        logger.debug("added filter")

    def _add_derived_param(self, dparam_dict):
        """Add dparam_dict to our dparam_rules
        dparam_dict: dict of the parameters needed to create the derived parameter"""
        self["dparam_rules"].append(dparam_dict)
        logger.debug("added derived parameter")

    def _add_event(self, event_name, event_dict):
        """Add rules for event definition to our storage"""
        self["event_rules"][event_name] = event_dict
        logger.debug("adding event %s" % (event_name))

    def _publish_version(self):
        """Increment version number"""
        self["version"] += 1
        logger.debug("version now %s" % (str(self['version'])))

    def load_from_json(self, json_file):
        """
        The standard method for loading data from an existing json dict.
        :param json_file: the json dict containing data to be loaded into our DStream
        :type json_file: dict
        """
        for key in json_file.keys():
            if key != 'stream_token':
                self[key] = json_file[key]
                logger.debug("added key %s" % (key))

    def define_dstream(self, storage_rules, ingestion_rules, source_dict, measure_list, field_names, user_id_names,
                      tag_list, filter_list, dparam_rule_list, event_list):
        """
        Old method for filling DStream from input data if not already in dict form.
        :param storage_rules: dict containing storage rules
        :type storage_rules: dict
        :param ingestion_rules: dict containing ingestion rules
        :type ingestion_rules: dict
        :param measure_list: list of tuples: (measure_name, dtype) for each measure supplied to the
        stream
        :type measure_list: list
        :param field_names: list of field names
        :type field_names: list
        :param user_id_names: list of user_id names
        :type user_id_names: list
        :param tag_list: list of tags
        :type tag_list: list
        :param filter_list: list of filter rules which are dicts
        :type filter_list: list of dict
        :param dparam_rule_list: list of dparam_rules, which are dicts
        :type dparam_rule_list: list of dict
        :param event_list: list of tuples: (event_name, event_rules)
        :type event_list: list of tuples
        """
        logger.warning("Don't use this method, it is cumbersome")
        self["storage_rules"] = storage_rules
        self["ingest_rules"] = ingestion_rules
        for key in source_dict.keys():
            self._add_source(key, source_dict[key])

        for measure_name, dtype in measure_list:
            self._add_measure(measure_name, dtype)

        for field in field_names:
            self._add_field(field)

        for uid in user_id_names:
            self._add_user_id(uid)

        for tag_name in tag_list:
            self._add_tag(tag_name)

        self["filters"].extend(filter_list)
        self["dparam_rules"].extend(dparam_rule_list)

        for event_name, event_rules in event_list:
            self._add_event(event_name, event_rules)

        self["version"] += 1
