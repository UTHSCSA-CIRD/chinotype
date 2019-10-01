#!/usr/bin/env python
'''
Chinotype back-end script, RC v1.1.0
Create counts and prevalences for ranking patient cohorts
   by relative prevalence of concepts. The PSID argument 
   in the usage hints below is the value in
   the RESULT_INSTANCE_ID column of the 
   i2b2demodata.QT_QUERY_RESULT_INSTANCE table, and it's
   the default name the web plugin gives to your cohort
   of interest. Note that one query will usually produce
   several RESULT_INSTANCE_ID's. You should use the one 
   whose RESULT_TYPE_ID is 1, because that's the result
   corresponding to a patient-set. The 4's are patient
   counts.

Usage:
   chinotype.py [options][-f PATTERN]... -m QMID
   chinotype.py [options][-f PATTERN]... -p PSID
   chinotype.py [options][-f PATTERN]... -t PSID -r PSID

Options:
    -h --help           Show this screen
    -m QMID             Query master ID to test, TOTAL population as reference
    -p PSID             Patient set ID to test, TOTAL population as reference
    -t PSID             Patient set ID to test (can be multiple separated by commas)
    -r PSID             Patient set ID for reference
    -v --verbose        Verbose/debug output (show all SQL)
    -c --config=FILE    Configuration file [default: config.ini]
    -o --output         Save chi2 csv output file
    -w --write=FILE     What file to save to? [default: output.csv]
    -j --json           Return JSON output
    -e --exists         Return extant data only; do not create new data columns
    -n LIMIT            Output only LIMIT rows of over/under represented facts
    -f PATTERN          Filter output concept codes by PATTERN (e.g. i2b2metadata.SCHEMES.C_KEY)
    -x CUTOFF           Filter output where reference population patient/fact count >= CUTOFF

QMID is the query master ID (from i2b2 QT tables). The latest query 
instance/result for a given QMID will be used.

PSID is the result instance ID (from i2b2 QT tables). 
'''
from sys import argv
from docopt import docopt
from ConfigParser import SafeConfigParser
import cx_Oracle as cx
from contextlib import contextmanager
import logging
import json
import re

log = logging.getLogger(__name__)
config_default = './config.ini'

def config(arguments={}):
    logging.basicConfig(format='%(asctime)s: %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S', level=logging.INFO)
    if arguments == {}:
        config_fn = config_default
    else:
        if arguments['--verbose']:
            log.setLevel(logging.DEBUG)
        config_fn = arguments['--config']
    cp = SafeConfigParser()
    cp.readfp(open(config_fn, 'r'), filename=config_fn)
    opt = cp._sections
    if arguments == {}:
        opt['qmid'] = None
        opt['psid'] = None
        opt['tpsid'] = None
        opt['rpsid'] = None
        opt['to_file'] = False
        opt['to_json'] = False
        opt['limit'] = None
        opt['filter'] = None
        opt['cutoff'] = None
        opt['exists'] = False
    else:
        opt['qmid'] = arguments['-m'] or None
        opt['psid'] = arguments['-p'] or None
        opt['tpsid'] = arguments['-t'] or None
        opt['rpsid'] = arguments['-r'] or None
        opt['to_file'] = arguments['--output'] or False
        opt['to_json'] = arguments['--json'] or False
        opt['outfile'] = arguments['--write'] or False
        opt['limit'] = arguments['-n'] or None
        if opt['limit'] == 'ALL' or opt['limit'] == 'all': opt['limit'] = None
        if opt['limit'] and not opt['limit'].isdigit():
            log.error('Invalid -n, --limit (must be integer): {0}'.format(opt['limit']))    
            foo = docopt(__doc__, argv=['--help'])
        opt['filter'] = list(set(arguments['-f'])) or []  # set removes duplicates
        opt['cutoff'] = arguments['-x'] or None
        opt['exists'] = arguments['--exists'] or False
    return opt


class Chi2:
    def __init__(self, listargs=[], args={}):
        if args == {}:
            args = docopt(__doc__, listargs)
        opt = config(args)
        db=opt['database']
        self.debug_dbopt(db)
        self.outfile = opt['outfile'] or opt['output']['csv'] # filename
        self.to_file = opt['to_file']  # write to file? T/F
        self.to_json = opt['to_json']  # return JSON output? T/F
        if self.to_file:
            log.info('output file={0}'.format(self.outfile))
        self.crc_host = db['crc_host']
        self.crc_port = db['crc_port']
        self.crc_user = db['crc_user']
        self.crc_service =  db['crc_service_name']
        self.crc_pw = db['crc_pw']
        self.branchnodes = db['chi_branchnodes']
        self.vfnodes = db['chi_vfnodes']
        self.allbranchnodes = db['chi_allbranchnodes']
        self.termtable = db['chi_termtable']
        self.schema = db['schema']
        self.chischemes = db['chischemes']
        self.metaschema = db['metaschema']
        self.qmid = opt['qmid']
        self.qiid = None
        self.qrid = None
        self.psid = opt['psid']
        self.psid_done = False
        self.tpsid = opt['tpsid'].split(',')
        self.rpsid = opt['rpsid']
        self.chi_host = db['chi_host']
        self.chi_port = db['chi_port']
        self.chi_user = db['chi_user']
        self.chi_service = db['chi_service_name']
        self.chi_pw = db['chi_pw']
        self.pconcepts = db['chi_pconcepts']
        self.pobsfact = db['chi_pobsfact']
        self.pcounts = db['chi_pcounts']
        self.chipats = db['chi_pats']
        self.chi_name = None
        self.pats = []
        self.out_json = None
        self.limit = opt['limit']
        self.filter = opt['filter']
        self.cutoff = opt['cutoff']
        self.extant = opt['exists']  # return extant data only
        self.ref = 'TOTAL'  # default reference patient set
        self.status = ''
        self.prepChi()      # create the chi2 tables if needed


    def debug_dbopt(self, db):
        log.debug('      data host={0}'.format(db['crc_host']))
        log.debug('      data port={0}'.format(db['crc_port']))
        log.debug('   data service={0}'.format(db['crc_service_name']))
        log.debug('      data user={0}'.format(db['crc_user']))
        log.debug('       chi host={0}'.format(db['chi_host']))
        log.debug('       chi port={0}'.format(db['chi_port']))
        log.debug('    chi service={0}'.format(db['chi_service_name']))
        log.debug('       chi user={0}'.format(db['chi_user']))
        log.debug('  chi pconcepts={0}'.format(db['chi_pconcepts']))
        log.debug('  chi pobsfact={0}'.format(db['chi_pobsfact']))
        log.debug('    chi pcounts={0}'.format(db['chi_pcounts']))
        log.debug('       chi pats={0}'.format(db['chi_pats']))
        log.debug('    data schema={0}'.format(db['schema']))
        log.debug('data metaschema={0}'.format(db['metaschema']))
        log.debug('   branch nodes={0}'.format(db['chi_branchnodes']))
        log.debug('valueflag nodes={0}'.format(db['chi_vfnodes']))
        log.debug('all branch nodes={0}'.format(db['chi_allbranchnodes']))
        log.debug('     term table={0}'.format(db['chi_termtable']))


    def getCrcOpt(self):
        return self.crc_host, self.crc_port, self.crc_service, self.crc_user, self.crc_pw

    def getChiOpt(self):
        return self.chi_host, self.chi_port, self.chi_service, self.chi_user, self.chi_pw, self.chi_name

    def getOracleDBI(self, host, port, service, user, pw, temp_table=None):
        dsn = cx.makedsn(host, int(port), service_name=service)
        log.debug(dsn)
        def theDB(it=[]):
            if not it:
                it.append(cx.connect(user, pw, dsn))
            return it[0]
        dbi = self.dbmgr(theDB, temp_table)
        return dbi


    def runQMID(self):
        '''Run chi2 for an i2b2 query master id'''
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        host, port, service, user, pw = self.getCrcOpt()
        dbi = self.getOracleDBI(host, port, service, user, pw)
        with dbi() as db:
            sql = '''
                select ps.result_instance_id
                    , qi.query_instance_id, qm.query_master_id
                from {0}.qt_query_master qm
                join {0}.qt_query_instance qi
                    on qi.query_master_id = qm.query_master_id
                join {0}.qt_query_result_instance ri 
                    on ri.query_instance_id = qi.query_instance_id
                join {0}.qt_patient_set_collection ps
                    on ps.result_instance_id = ri.result_instance_id
                where ri.result_type_id = 1     -- patient set
                and qm.query_master_id={1} and rownum = 1
                order by ps.result_instance_id desc, qi.query_instance_id desc
            '''.format(self.schema, self.qmid)
            cols, rows = do_log_sql(db, sql)
            if len(rows) == 0:
                str = 'ERROR, QMID {0} has no patient set result instance'.format(self.qmid)
                #log.error(str)
                return str
            qdata = dict(zip([c.lower() for c in cols], list(rows[0])))
            log.debug('qdata={0}'.format(qdata))
            self.qiid = qdata['query_instance_id']
            self.qrid = qdata['result_instance_id']
            self.chi_name = 'M{0}_I{1}_R{2}'.format(self.qmid, self.qiid, self.qrid)

            sql = '''
                select distinct patient_num
                from {0}.qt_patient_set_collection pc
                join {2} chipat on chipat.pn = pc.patient_num
                where result_instance_id = {1}
            '''.format(self.schema, self.qrid, self.chipats)
            cols, rows = do_log_sql(db, sql)
            self.pats = rows

        return self.runChi()

    def runPSID_p2(self):
        if self.rpsid in self.tpsid:
            self.status = 'Job canceled, identical patient sets'
            if self.to_json:
                self.status = json.dumps({'cols': [], 'rows': [], 'status': self.status})
        elif not self.checkIntersection():
            if self.to_json:
                self.status = json.dumps({'cols': [], 'rows': [], 'status': self.status})
        else:
            # do the reference patient set first
            self.resetPS(self.rpsid)
            self.runPSID()
            ref = self.chi_name
            if self.extant and ref is None:
                if self.to_json:
                    self.status = json.dumps({'cols': [], 'rows': [], 'status': self.status})
            else:
                # then do the test patient set, using the reference column name
                self.ref = ref
                for ii in self.tpsid:
		    self.resetPS(ii)
		    self.runPSID()
                if self.extant and self.chi_name is None:
                    if self.to_json:
                        self.status = json.dumps({'cols': [], 'rows': [], 'status': self.status})
        return self.status

    def resetPS(self, psid):
        self.psid = psid
        self.psid_done = False
        self.qmid = None
        self.qiid = None
        self.qrid = None
        self.chi_name = None
        self.ref = None

    def runPSID(self):
        '''Run chi2 for an i2b2 patient set id'''
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        host, port, service, user, pw = self.getCrcOpt()
        dbi = self.getOracleDBI(host, port, service, user, pw)
        with dbi() as db:
            # First check if chi2 results exists for patient set already
            try:
                log.debug('Checking if columns already exist for PSID {0}...'.format(self.psid))
                table_info = pcounts.split('.')
                owner, table_name = '', ''
                if len(table_info) > 1:
                    owner = 'and owner = \'{0}\''.format(table_info[0].upper())
                    table_name = 'and table_name = \'{0}\''.format(table_info[1].upper())
                elif len(table_info) > 0:
                    table_name = 'and table_name = \'{0}\''.format(table_info[0].upper())
                sql = '''
                select column_name from all_tab_columns
                where 1=1 {0} {1}
                and column_name like '%_R{2}'
                order by column_name desc
                '''.format(owner, table_name, self.psid)
                cols, rows = do_log_sql(db, sql)
                if len(rows) > 0:
		    import pdb; pdb.set_trace()
                    self.chi_name = rows[0][0]
                    self.psid_done = True
                    log.info('Using preexisting chi columns for PSID {0}'.format(self.psid))
                elif self.extant:
                    self.status = 'No data for PSID {0}, try running without -e/--exists'.format(self.psid)
                    return self.status
            except:
                raise

            # Get QMID and QIID to make column names
            if self.chi_name is None:
                # Make sure patient set exists in i2b2
                sql = '''
                    select qm.query_master_id
                        , qi.query_instance_id
                        , ri.result_instance_id
                    from {0}.qt_query_result_instance ri
                    join {0}.qt_query_instance qi 
                        on qi.query_instance_id = ri.query_instance_id
                    join {0}.qt_query_master qm 
                        on qm.query_master_id = qi.query_master_id
                    where ri.result_type_id = 1     -- patient set
                    and ri.result_instance_id = {1} and rownum = 1
                    order by qi.query_instance_id desc, qm.query_master_id desc
                '''.format(self.schema, self.psid)
                cols, rows = do_log_sql(db, sql)
                if len(rows) == 0:
                    str = 'ERROR, patient set (PSID={0}) not found in QT tables'.format(self.psid)
                    #log.error(str)
                    return str
                qdata = dict(zip([c.lower() for c in cols], list(rows[0])))
                log.debug('qdata={0}'.format(qdata))
                self.qmid = qdata['query_master_id']
                self.qiid = qdata['query_instance_id']
                self.qrid = qdata['result_instance_id']
                self.chi_name = 'M{0}_I{1}_R{2}'.format(self.qmid, self.qiid, self.qrid)
            else:
                match = re.match('M(?P<qmid>\d+)_I(?P<qiid>\d+)_R(?P<qrid>\d+)', self.chi_name)
                self.qmid = match.group('qmid')
                self.qiid = match.group('qiid')
                self.qrid = match.group('qrid')

            sql = '''
                select distinct patient_num
                from {0}.qt_patient_set_collection pc
                join {2} chipat on chipat.pn = pc.patient_num
                where result_instance_id = {1}
            '''.format(self.schema, self.qrid, self.chipats)
            cols, rows = do_log_sql(db, sql)
            self.pats = rows

            return self.runChi()


    def prepChi(self):
        schema = self.schema
        metaschema = self.metaschema
        pconcepts = self.pconcepts
        pobsfact = self.pobsfact
        pcounts = self.pcounts
        chischemes = self.chischemes
        host, port, service, user, pw, temp_table = self.getChiOpt()
        chi_dbi = self.getOracleDBI(host, port, service, user, pw, temp_table)
        with chi_dbi() as db:
            # check if chipats exists
            try:
                log.debug('Checking if chi_pats table exists...')
                cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(self.chipats))
            except:
                log.info('chi_pats table ({0}) does not exist, creating it...'.format(self.chipats))
                sql = '''
                -- patients who have at least some visit info, on which we will filter using joins
                -- question: are there any patients missing this code that nevertheless have EMR
                -- data other than demographics?
                create table {0} as
                select distinct patient_num pn
                from {1}.observation_fact
                -- where concept_cd like 'KUMC|DischargeDisposition:%'
                '''.format(self.chipats, schema)
                cols, rows = do_log_sql(db, sql)
                sql = '''
                alter table {0} add primary key (pn)
                '''.format(self.chipats)
                cols, rows = do_log_sql(db, sql)
            # check if pconcepts exists
            try:
                log.debug('Checking if chi_pconcepts table exists...')
                cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(pconcepts))
            except:
                log.info('chi_pconcepts table ({0}) does not exist, creating it...'.format(pconcepts))
                sql = '''
                create table {0} as

                -- your basic list of distinct patients and raw concept codes from the datamart (1)
                select patient_num pn, concept_cd ccd
                from {1}.observation_fact obs
                join {2} chipat on chipat.pn = obs.patient_num
                union

                -- distinct patients and certain branch nodes, as gathered from the ontology (3).(4)
                select obs.patient_num pn, c_basecode ccd 
                from {3}.{4}  
                join {1}.concept_dimension cd  		-- use obs_fact
                on concept_path like c_dimcode||'%' 
                join {1}.observation_fact obs 		-- use obs_fact
                on cd.concept_cd = obs.concept_cd 	-- use obs_fact
                join {2} chipat on chipat.pn = obs.patient_num
                -- selection criteria for specific types of branch nodes
                where ( {5} or {6} ) and 
                -- selection criteria affecting all branch nodes
                {7}
                union

                -- same as above, but facts that are outside their reference 
                -- ranges, i.e. labs
                select patient_num pn,valueflag_cd||'_'||c_basecode ccd 
                from {3}.{4}  
                join {1}.concept_dimension cd  		-- use obs_fact
                on concept_path like c_dimcode||'%' 
                join {1}.observation_fact obs 		-- use obs_fact
                on cd.concept_cd = obs.concept_cd 	-- use obs_fact
                join {2} chipat on chipat.pn = obs.patient_num
                where 
                ( {6} ) and
                -- {7} and 
                valueflag_cd != '@'
                '''.format(pconcepts, self.schema, self.chipats, self.metaschema, self.termtable, self.branchnodes, self.vfnodes, self.allbranchnodes)
                cols, rows = do_log_sql(db, sql)
                sql = '''
                alter table {0} add primary key (ccd,pn)
                '''.format(pconcepts)
                cols, rows = do_log_sql(db, sql)
                #sql = '''
                #create unique index {0}_pncd_idx on {0} (pn,ccd)
                #'''.format(pconcepts)
                #cols, rows = do_log_sql(db, sql)
            # create pcounts table if needed
            try:
                log.debug('Checking if chi_pcounts table exists...')
                cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(pcounts))
            except:
                log.info('chi_pcounts table ({0}) does not exist, creating it...'.format(pcounts))
                sql = '''select count(*) from {0}'''.format(self.chipats)
                cols, rows = do_log_sql(db,sql)
                pat_totalcount = rows[0][0] # now this takes less than 3 minutes
                sql = '''
                -- pcounts = {0}
                -- pconcepts = {1}
                -- schema = {2}
                -- self.metaschema = {3} 
                -- self.termtable = {4}
                -- self.branchnodes = {5}
                -- self.vfnodes = {6}
                -- self.allbranchnodes = {7}
                -- pat_totalcount = {8}
                create table {0} as
                --- This seems to be the subquery that creates counts by concept
                with ttls as (
		  select ccd, replace(replace(ccd,'H_',''),'L_','') joinccd
		  ,count(distinct pn) total from {1} 
		  group by ccd
		), ttls2 as (
		  select ccd, total from ttls where ccd like 'LOINC:%'
		)
		select 
		case 
		  when ttls.ccd like 'NAACCR|%' then 'NAACCR'
		  when instr(joinccd, ':') > 0 then 
		      substr(joinccd, 1, instr(joinccd, ':')-1)
		  else joinccd
		  end prefix
		, ttls.ccd
                --select prefix, ccd
                , case 
		  when ttls.ccd like 'H\_%' escape '\\' then '[ABOVE REFERENCE] '||name
		  when ttls.ccd like 'L\_%' escape '\\' then '[BELOW REFERENCE] '||name
		  when ttls.ccd like 'A\_%' escape '\\' then '[ABNORMAL] '||name
		  else name
		end name
		/*
		ttls.total is one of these...
		the total patients with high values if t2.total is not NULL (so t2.total is the denominator)
		the total patients with low values if t3.total is not NULL (so t3.total is the denominator)
		the total patients with abnormal values if t4.total is not NULL (so t4.total is the denominator)
		the total number of patients having the lab done if all tX.totals null (grand total should be the denominator)
		definitely not a hardcoded value! wtf was I thinking
		*/
		, ttls.total, ttls.total/coalesce(t2.total,t3.total,t4.total,{8}) frc_total 
		from ttls

		-- dead code
                /*from (
                    select ccd, replace(replace(ccd,'H_',''),'L_','') joinccd
                    , case 
                        when ccd like 'NAACCR|%' then 'NAACCR'
                        when instr(ccd, ':') > 0 then 
			  replace(replace(substr(ccd, 1, instr(ccd, ':')-1),'H_',''),'L_','')
                        else ccd
                    end prefix
                    , count(distinct pn) total
                    --, count(distinct pn) / (select count(distinct pn) from {1}) frc_total
                    , count(distinct pn) / {8} frc_total
                    from {1} 
                    group by ccd
                ) chicon */
                -- end dead code

                left join ttls2 t2 on ttls.ccd = 'H_'||t2.ccd
                left join ttls2 t3 on ttls.ccd = 'L_'||t3.ccd
                left join ttls2 t4 on ttls.ccd = 'A_'||t4.ccd
                left join (
                    select concept_cd, min(name) name
                    from (
		      select c_basecode concept_cd,c_name name from {3}.{4}
		      where ({5} or {6}) and {7}
		      union
		      select concept_cd,name_char name from {2}.concept_dimension
		      )
                    group by concept_cd
                ) cd on cd.concept_cd = ttls.joinccd
		-- are we eliminating some rare but important fact by setting a hard lower limit of 10 facts?
		-- hopefully not
		where ttls.total > 10
                union all
                select 'TOTAL' prefix, 'TOTAL' ccd, 'All Patients in Population' name
                , {8} total, 1 frc_total from dual
                '''.format(pcounts, pconcepts, schema, self.metaschema, self.termtable, self.branchnodes, self.vfnodes, self.allbranchnodes, pat_totalcount)
                cols, rows = do_log_sql(db, sql)

                # Why do we need the total column in the index?
                #sql = '''alter table {0} add constraint {0}_pk primary key (prefix,ccd,total)
                sql = '''alter table {0} add primary key (prefix,ccd)
                '''.format(pcounts)
                cols, rows = do_log_sql(db, sql)
                # prefix is for filtering the output, so needs an index
                try:
                    cols, rows = do_log_sql(db,'drop index {0}_ccd_idx'.format(pcounts))
                except:
                    pass
                sql = '''create bitmap index {0}_ccd_idx on {0} (ccd)
                '''.format(pcounts)
                cols, rows = do_log_sql(db, sql)
                # total is used for filtering by threshold, so needs an index
                try:
                    cols, rows = do_log_sql(db,'drop index {0}_tl_idx'.format(pcounts))
                except:
                    pass
                sql = '''
                create index {0}_tl_idx on {0} (total)
                '''.format(pcounts)
                cols, rows = do_log_sql(db, sql)
	    try:
		log.debug('Checking if empirical schemes table exists...')
		cols, rows = do_log_sql(db,'select 1 from {0} where rownum = 1'.format(chischemes))
	    except:
		log.info('chi_schemes table ({0}) does not exist, creating it...'.format(chischemes))
		sql = '''
		create table {0} as
		select c_key, prefix c_name, c_description 
		from (select distinct prefix from {1}) pct
		left join {2}.schemes
		on prefix = {2}.schemes.c_name
		where prefix is not null
		'''.format(chischemes,pcounts,metaschema)
		cols, rows = do_log_sql(db,sql)
		sql = '''
		update {0} set c_key = c_name where c_key is null
		'''.format(chischemes)
		cols, rows = do_log_sql(db,sql)
		do_log_sql(db,'commit')
		# Make all c_keys :-terminated if not already
		# TODO: test!
		sql = '''
		update {0} set c_key = c_key||':' where c_key not like '%:'
		'''.format(chischemes)
		cols, rows = do_log_sql(db,sql)
		do_log_sql(db,'commit')
		sql = '''
		update {0} set c_description = c_name where c_description is null
		'''.format(chischemes)
		cols, rows = do_log_sql(db,sql)
		do_log_sql(db,'commit')
		sql = '''
                alter table {0} add primary key (c_name)
                '''.format(chischemes)
                cols, rows = do_log_sql(db, sql)

                #try:
                    #cols, rows = do_log_sql(db,'drop index {0}_idx'.format(chischemes))
                #except:
                    #pass
		#sql = '''create index {0}_idx on {0} (c_name)'''.format(chischemes)
		#cols, rows = do_log_sql(db,sql)


    def runChi(self):
        pats = self.pats
        schema = self.schema
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        host, port, service, user, pw, temp_table = self.getChiOpt()
        chi_dbi = self.getOracleDBI(host, port, service, user, pw, temp_table)
        with chi_dbi() as db:
            runChi = True
            if self.qmid is not None:
                col_name = self.checkRerunQMID(db)
                if col_name != '' or self.extant:
                    self.chi_name = col_name # already done, but col name may differ
                    runChi = False
            elif self.psid is not None and self.psid_done:
                runChi = False              # already done
            chi_name = self.chi_name

            if runChi:
                # make a temp table of patient set for query chi_name=m###_r###_i###
                # why are we looking at PATIENT_DIMENSION? Don't we already have chi_pats?
                log.info('Creating chi columns for PSID {0}'.format(self.psid))
                log.debug('Creating temp table for patient set...')
                sql = '''
                    create table {0} as 
                        select patient_num pn
                        --from {1}.patient_mapping
                        from {1}.patient_dimension
                        where 1 = 0
                '''.format(chi_name, schema)
                cols, rows = do_log_sql(db, sql)
                sql='insert into {0} (pn) values (:pn)'.format(chi_name)
                cols, rows = do_log_sql(db, sql, [[p[0]] for p in pats])

                # add columns to chi_pcounts
                sql = 'alter table {0} add {1} number'.format(pcounts, chi_name)
                cols, rows = do_log_sql(db, sql)

                sql = 'alter table {0} add frc_{1} number'.format(pcounts, chi_name)
                cols, rows = do_log_sql(db, sql)
                log.info('Updating view for correlated update')
                sql = '''
                -- pconcepts = {0}
                -- chi_name = {1}
                create or replace view new_cohort as
		with c1 as (
		  -- select cohort of interest from test_pconcepts table
		  select ccd      -- concept code
		  , count(distinct mc.pn) cnt  -- count
		  from {0} pc join {1} mc on mc.pn = pc.pn
		  group by ccd
		), c2 as (select ccd,cnt denom from c1 where ccd like 'LOINC:%')
		select c1.ccd,min(cnt) cnt,min(c2h.denom) hdenom,min(c2l.denom) ldenom from
		c1 left join c2 c2h on c1.ccd = 'H_'||c2h.ccd
		left join c2 c2l on c1.ccd = 'L_'||c2l.ccd
		group by c1.ccd
		'''.format(pconcepts,chi_name)
		cols, rows = do_log_sql(db,sql)
		# This view-based approach seems to run in under 2min for a 19k patient-set
		log.info('updating columns of {0}'.format(pcounts))
                sql = '''
                -- chi_name = {0}
                -- pcounts = {1}
                -- len(pats) = {2}
                update (
                    select 
                        pc.{0} emptycnt -- empty target column for counts
                        , coalesce(cnt,0) newcnt -- source column for counts
                        , pc.frc_{0} emptyfrc -- empty target column for fractions
                        , coalesce(cnt/coalesce(hdenom,ldenom,{2}),0) newfrc -- source column for fractions
                    from {1} pc 
                    left join new_cohort on pc.ccd = new_cohort.ccd
                ) up
                set up.emptycnt = up.newcnt, up.emptyfrc = up.newfrc
                '''.format(chi_name, pcounts, len(pats))
                cols, rows = do_log_sql(db, sql)

                sql = '''
                update {0} set {1} = {2}
                , frc_{1} = 1
                where ccd = 'TOTAL'
                '''.format(pcounts, chi_name, len(pats))
                cols, rows = do_log_sql(db, sql)

                cols, rows = do_log_sql(db, 'commit')
                cols, rows = do_log_sql(db, 'drop table {0}'.format(chi_name))

                if self.to_json:
                    sql = 'select {0}, {1} from {2}'.format(chi_name, 'frc_%s' % chi_name, pcounts)
                    cols, rows = do_log_sql(db, sql)

            if self.ref:
                resp = self.chi2_output(db)
            else:
                resp = ''

        log.info('patient count={0}'.format(len(pats)))
        log.info('chi_pconcepts={0}'.format(pconcepts))
        log.info('chi_pcounts={0}'.format(pcounts))
        log.info('chi_name={0}'.format(chi_name))
        return resp

    
    def checkRerunQMID(self, db):
        '''Check if results already exists for QMID'''
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        qmid = self.qmid
        chi_name = self.chi_name
        schema = self.schema
        pats = self.pats
        # if pcounts has QMID & patient count matches latest, return existing results
        # if pcounts has QMID & patient count DOES NOT match latest, warn/exit
        try:
            log.debug('Checking if columns already exist for QMID {0}...'.format(qmid))
            table_info = pcounts.split('.')
            owner, table_name = '', ''
            if len(table_info) > 1:
                owner = 'and owner = \'{0}\''.format(table_info[0].upper())
                table_name = 'and table_name = \'{0}\''.format(table_info[1].upper())
            elif len(table_info) > 0:
                table_name = 'and table_name = \'{0}\''.format(table_info[0].upper())
            sql = '''
            select column_name from all_tab_columns
            where 1=1 {0} {1}
            and column_name like 'M{2}_%'
            '''.format(owner, table_name, qmid)
            cols, rows = do_log_sql(db, sql)
            if len(rows) > 0:
                column_name = rows[0][0]
                cols, rows = do_log_sql(db, \
                    'select {0} total from {1} where ccd = \'TOTAL\''.format(chi_name, pcounts))
                log.debug('      total: {0}'.format(rows[0][0]))
                if len(pats) == rows[0][0]:
                    # In practice, i2b2 query re-runs seem to always get a new QMID,
                    # but this should catch duplicate requests for chi2 calculation
                    log.debug('WARNING, preexisting chi columns for QMID {0}'.format(qmid))
                    log.debug('  new_query: {0}'.format(chi_name))
                    log.debug('  old_query: {0}'.format(column_name))
                else:
                    # This should never happen unless i2b2 QT table are corrupt 
                    # or out of sync with the chi_pcounts table
                    log.info('ERROR, columns exist for QMID {0} but patiet set differs'.format(qmid))
                    log.info('  new_query: {0}'.format(chi_name))
                    log.info('  old_query: {0}'.format(column_name))
                return column_name
            else:
                # pcounts does not have existing QMID columns
                return ''
        except:
            raise

    def checkIntersection(self):
        host, port, service, user, pw = self.getCrcOpt()
        dbi = self.getOracleDBI(host, port, service, user, pw)
        with dbi() as db:
            try:
                sql = '''
                select count(*) from (
                    select patient_num from {0}.qt_patient_set_collection
                    where result_instance_id in {1} -- test
                    minus
                    select patient_num from {0}.qt_patient_set_collection
                    where result_instance_id = {2} -- ref
                )
                '''.format(self.schema, 
			   tuple(self.tpsid) if len(self.tpsid)>1 else '('+str(self.tpsid[0])+')', 
			   self.rpsid)
                cols, rows = do_log_sql(db, sql)
                if rows[0][0] > 0:
                    self.status = 'Job canceled, all patients in test subset must be in the reference set'
                    return False
            except:
                raise
        return True

    def dbmgr(self, connect, temp_table=None):
        '''Make a context manager that yields cursors, given connect access.
        '''
        @contextmanager
        def dbtrx():
            conn = connect()
            cur = conn.cursor()
            try:
                yield cur
            except Exception as e:
                #error, = e.args
                #log.debug('e.args={0}'.format(e.args))
                conn.rollback()
                if temp_table:
                    try:
                        log.debug('Previous query rollback pending, dropping temp table...')
                        cols, rows = do_log_sql(cur, 'drop table {0}'.format(temp_table))
                    except:
                        pass
                    finally:
                        log.debug('Raising error from rollback...')
                raise e
            else:
                conn.commit()
            finally:
                cur.close()
        return dbtrx

    def getFilterSql(self):
	# Should this be select c_key ?
        sql = 'select c_name from {0}'.format(self.chischemes)
        if 'ALL' in self.filter:
            sql += ' where 1=1'
        elif len(self.filter) > 0:
            sql += '\nwhere 1=0'
        # Shouldn't this be indented to be part of elif block?
        for p in range(0, len(self.filter)):
            if self.filter[p] != 'ALL':
                sql += '\nor c_key like :{0} || \'%\''.format(p)
        return sql

    def chi2_output(self, db):
        if (self.chi_name is None or self.chi_name == '') and self.extant:
            # This should only happen for QMID 
            self.status = 'No data for QMID {0}, try running without -e/--exists'.format(self.psid)
            if self.to_json:
                self.status = json.dumps({'cols': [], 'rows': [], 'status': status})
            return self.status
        # Skip filtering/output if not required
        if not self.to_file and not self.to_json: 
            if len(self.filter) > 0:
                log.info('Ignoring filters, no ouput format selected')
            self.status = 'Done, chi success!'
            return self.status
        # Limit results by number of rows results
        limstr = ''
        if self.limit:
            limstr = 'where rank <= {0} or revrank <= {0}'.format(self.limit)
        # Filter results by concept code prefix (data domain)
        filterStr = self.getFilterSql()
        if len(self.filter) > 0:
            log.info('Filters: {0}'.format(self.filter))
            if 'ALL' in self.filter: self.filter.remove('ALL')
            cols, rows = do_log_sql(db, filterStr, self.filter)
            log.info('Applied filters prefixes: {0}'.format([r[0] for r in rows]))
        # Filter results by reference fact cutoff
        cutoff = ''
        if self.cutoff:
            cutoff = 'and {0} >= {1}'.format(self.ref, self.cutoff)
            log.info('Reference patient set cutoff: {0}'.format(self.cutoff))
        # Store prefixes for web UI concepts-selector drop down box
        prefixes = []
        if self.to_json:
            sql = '''
            select c_name name, c_description description
            from {0}
            order by c_name
            '''.format(self.chischemes)
            cols, rows = do_log_sql(db, sql)
            prefixes = [(r[0], r[1]) for r in rows]
        # NOTE: comparing to enclosing population might actually mean that the
        # chi squared test for goodness of fit should be used rather than the
        # test for independence... 
        # http://www.ablongman.com/graziano6e/text_site/MATERIAL/statconcepts/chisquare.htm
        # Get results data 
        sql = '''
        with patterns as (
            {4}
        )
        , cohort as (
            select {0} pat_count from {1} where ccd = 'TOTAL'
        )
        , data as (
            select prefix, ccd
            , name
            , {3}
            , frc_{3} 
            , {0}
            , frc_{0}
            -- , power({0} - (cohort.pat_count * frc_{3}), 2) / (cohort.pat_count * frc_{3}) chisq
            -- oops, that's not really chisq df=1, but the below is...
            , case 
	      when frc_{3} = frc_{0} then 0
	      when frc_{3} = 1 or frc_{0} = 1 then null
	      else
	      power({0} - (cohort.pat_count * frc_{3}), 2)*(1/(cohort.pat_count * frc_{3}) + 
	      1/((cohort.pat_count-{0}) * frc_{3}) + 1/(cohort.pat_count * (1-frc_{3})) + 
	      1/((cohort.pat_count-{0}) * (1-frc_{3}))) 
	      end chisq
	    , case 
	      when frc_{0}=frc_{3} then 1 
	      when frc_{0} in (0,1) or frc_{3} in (0,1) then 0
	      else
	      (1-frc_{3})*frc_{0}/((1-frc_{0})*frc_{3}) 
	      end odds_ratio
	    , case when frc_{3} = frc_{0} then 0 when frc_{3} < frc_{0} then 1 else -1 end dir
            from {1}
            , cohort
            where frc_{3} > 0   -- reference patient set frequency
            {5}
            --where frc_{3} > 0 or frc_{0} > 0
        )
        , ranked_data as (
            select data.*
            --, row_number() over (order by chisq*dir desc) as rank
            --, row_number() over (order by chisq*dir asc) as revrank  -- this is not a useless line
            , row_number() over (order by odds_ratio desc) as rank
            , row_number() over (order by odds_ratio asc) as revrank  -- this is not a useless line
            from data   
            join patterns on data.prefix = patterns.c_name 
            where ccd != 'TOTAL'
            order by rank
        ) 
        select prefix, ccd, name, {3}, frc_{3}, {0}, frc_{0}, chisq, odds_ratio, dir
        from data where ccd = 'TOTAL'
        union all
        select prefix, ccd, name, {3}, frc_{3}, {0}, frc_{0}, chisq, odds_ratio, dir
        from ranked_data {2}
        '''.format(self.chi_name, self.pcounts, limstr, self.ref, filterStr, cutoff)
        cols, rows = do_log_sql(db, sql, self.filter)

        # Write results to file
        if self.to_file:
            quote = ['PREFIX', 'CCD', 'NAME']
            with open(self.outfile, 'w') as file:
                file.write('%s\n' % ','.join(['\"{0}\"'.format(c) for c in cols]))
                for row in rows:
                    data = dict(zip(cols, row))
                    for k, v in sorted(data.items(), key=lambda x: cols.index(x[0])):
                        if k in quote:
                            # clean up embedded single quotes
                            file.write('\"{0}\"'.format((v or '').replace('"',"'"))) 
                        else:
			    # why not: file.write(str(v))   ...?
                            file.write('{0}'.format(v or '')) 
                        if k == cols[-1]:
                            file.write('\n')
                        else:
                            file.write(',')

        # Return results/status
        status = 'Done, chi success!'
        if self.to_json:
            self.status = json.dumps({'cols': cols, 'rows': rows, 'prefixes': prefixes, 'status': status})
        else:
            self.status = status
        return self.status


def do_log_sql(cur, sql, params=[]): 
    '''Execute sql on given connection and log it
    '''
    cols, rows = None, None
    if len(params) > 1 and sql.strip().lower().startswith('insert'):
        log.debug('executemany: {0}'.format(sql))
        cursor = cur.executemany(sql, params)
    else:
        log.debug('    execute: {0}'.format(sql))
        cursor = cur.execute(sql, params)
    if cursor:
        if cursor.description:
            cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        if len(rows) > 0:
            log.debug('   rowcount: {0}'.format(len(rows)))
        elif cursor.rowcount:
            log.debug('   rowcount: {0}'.format(cursor.rowcount))
    else:
        log.debug('   rowcount: None')
    return cols, rows


if __name__=='__main__':
    args = docopt(__doc__, argv=argv[1:])
    if args['-p']:
        log.info(Chi2(args=args).runPSID())
    elif args['-m']:
        log.info(Chi2(args=args).runQMID())
    elif args['-t'] and args['-r']:
        log.info(Chi2(args=args).runPSID_p2())

