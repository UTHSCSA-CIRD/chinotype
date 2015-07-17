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
	psetexists = cr.execute("select count(*) from all_objects where object_type in ('TABLE','VIEW') and object_name = '"+par['pset']+"'").fetchone()[0]
	print psetexists
	pns = [ii[0] for ii in cr.execute('select distinct patient_num from '+par['datamart']+'.qt_patient_set_collection where result_instance_id = '+pset).fetchall()]
	np = len(pns)
	print np 
	""" --Here is the query that this will eventually wrap:
	update (
        	with cnts as ( select 
                        -- concept code
                        ccd 
                        -- how many patients in cohort 
                        ,count(*) cnt 
                        -- what fraction of patients in cohort 
                        ,count(*)/"+np+" frc 
                -- this join is used for selecting just the cohort of interest out of the ${pcname}$ table
                from "+par['pset']+" pc where pn in ("+",".join(pns)+")
        	)
	        -- to avoid spam, we select just the source and target columns for the update
        	select pc."+newcolname+" emptycnt -- empty target column for counts
                ,nvl(cnts.cnt,0) newcnt -- source column for counts
                ,pc.frc_"+newcolname+" emptyfrc -- empty target column for fractions
                ,nvl(cnts.frc,0) newfrc -- source column for fractions
        	from par['pccnts'] pc left join cnts on pc.ccd = cnts.ccd
	) up
	-- and then do the update
	set up.emptycnt = up.newcnt, up.emptyfrc = up.newfrc;

	-- Then we have one last statement, for the totals
	"+update par['pccnts']+" set "+newcolname+" = "+string(np)
        , frc_"+newcolname+" = 1
        where ccd = 'TOTAL';
	"""
	import pdb;pdb.set_trace()

if __name__ == '__main__':
	main(cn,cr,args.pset)
