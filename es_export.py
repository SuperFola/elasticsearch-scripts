#!/usr/bin/env python3
import elasticsearch as es
import colorama as colo
import os
import configparser as cfgparser
import pprint

# pretty printer useful pretty much in all the script
pprinter = pprint.PrettyPrinter(indent=4)
pp = lambda *args, **kwargs: pprinter.pprint(*args, **kwargs)

# read config from current working directory
config = cfgparser.ConfigParser()
config.read("elasticsearch.ini")

def run(args):
	"""
	Syntax: py -m MODULE export
		[--batch-size size>0]
		[--start index>0]
		[--directory name]
		[--filename name]
		[--start-id id]
		[-v|--verbosity]
	"""
	
	db = es.Elasticsearch(
		[
			{
				'host': config.get("Host", "name")
				, 'port': config.get("Host", "port")
				, 'use_ssl': config.getboolean("Host", "ssl")
			}
		]
		, verify_certs=config.getboolean("General", "verify certs")
		, ca_certs=config.get("General", "ca certs")
		, sniff_on_start=config.getboolean("Misc", "sniff on start")
	)
	
	if args.verbosity:
		pp(db.info())
	
	if db.info()['status'] != 200:
		print(f"We have a problem Houston ! Elastic search is returning {db.info()['status']} instead of 200 OK!")
		print("Contact the server administrator or double check the configuration of the script")
		return -1
	
	
	return 0