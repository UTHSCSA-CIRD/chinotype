""" Create or add to table of counts and prevalences which can later be used to rank by relative prevalence
---------------------------------------------------------------------
"""


import cx_Oracle as cx,argparse,ConfigParser

cfg = ConfigParser.RawConfigParser()
parser = argparse.ArgumentParser()
parser.add_argument("pset",help="A number, representing a patient set that exists in i2b2")
parser.add_argument("configfile",help="The config file from which to get connection information")
args = parser.parse_args()
# We read in the config file specified by the second argument
cfg.read(args.configfile)
par=dict(cfg.items("connection"))

dsn=cx.makedsn(par['host'],par['port'],service_name=par['service_name'])
cn=cx.connect(par['user'],par['pw'],dsn)
cr=cx.Cursor(cn)

def main(cn,cr,pset):
	""" The plan: 
	1. Connect to the datamart of choice (need to add more config variables to specify it)
	2. If not already existing, create a count-table in another schema of our choice (need configs for that)
	3. Add a pair of dynamically named new columns to that table (probably passed as an optional command-line argument
	   If no command-line argument somehow construct names from one of the QT_tables. Or maybe just pset_XXXX
	4. Populate those columns with a count and fraction of distinct patients grouped by CONCEPT_CD 
	5. Will need a separate function taking a pair of existing column-names as input and returning a ranked list of concepts
	. . .
	7. Disparities!
	"""
	print pset

if __name__ == '__main__':
	main(cn,cr,args.pset)
