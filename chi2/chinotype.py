#!/user/bin/env python
'''
Create counts and prevalences for ranking patient cohorts
   by relative prevalence of concepts.

Usage:
   driver.py [options] -m QMID
   driver.py [options] -p PSID
   driver.py [options] -t PSID -r PSID

Options:
    -h --help           Show this screen
    -m QMID             Query master ID to test, TOTAL population as reference
    -p PSID             Patient set ID to test, TOTAL population as reference
    -t PSID             Patient set ID to test
    -r PSID             Patient set ID for reference
    -v --verbose        Verbose/debug output (show all SQL)
    -c --config=FILE    Configuration file [default: config.ini]
    -o --output         Create an chi2 output file
    -n limit            Limit to topN over/under represented facts [default: 10]

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
        opt['qpsid'] = None
        opt['rpsid'] = None
        opt['to_file'] = False
        opt['limit'] = 10
    else:
        opt['qmid'] = arguments['-m']
        opt['psid'] = arguments['-p']
        opt['qpsid'] = arguments['-t']
        opt['rpsid'] = arguments['-r']
        opt['to_file'] = arguments['--output']
        opt['limit'] = arguments['-n']
    return opt


class Chi2:
    def __init__(self, arguments={}):
        opt = config(arguments)
        db=opt['database']
        self.debug_dbopt(db)
        self.outfile = opt['output']['csv'] # filename
        self.to_file = opt['to_file']  # write to file? T/F
        if self.to_file:
            log.info('output={0}'.format(self.outfile))
        self.crc_host = db['crc_host']
        self.crc_port = db['crc_port']
        self.crc_user = db['crc_user']
        self.crc_service =  db['crc_service_name']
        self.crc_pw = db['crc_pw']
        self.schema = db['schema']
        self.qmid = opt['qmid']
        self.qiid = None
        self.qrid = None
        self.psid = opt['psid']
        self.psid_done = False
        self.psid1 = opt['qpsid']
        self.psid2 = opt['rpsid']
        self.chi_host = db['chi_host']
        self.chi_port = db['chi_port']
        self.chi_user = db['chi_user']
        self.chi_service = db['chi_service_name']
        self.chi_pw = db['chi_pw']
        self.pconcepts = db['chi_pconcepts']
        self.pcounts = db['chi_pcounts']
        self.chi_name = None
        self.pats = []
        self.out_json = None
        self.limit = opt['limit']


    def debug_dbopt(self, db):
        log.debug('data host={0}'.format(db['crc_host']))
        log.debug('data port={0}'.format(db['crc_port']))
        log.debug('data service={0}'.format(db['crc_service_name']))
        log.debug('data user={0}'.format(db['crc_user']))
        log.debug('chi host={0}'.format(db['chi_host']))
        log.debug('chi port={0}'.format(db['chi_port']))
        log.debug('chi service={0}'.format(db['chi_service_name']))
        log.debug('chi user={0}'.format(db['chi_user']))
        log.debug('chi pconcepts={0}'.format(db['chi_pconcepts']))
        log.debug('chi pcounts={0}'.format(db['chi_pcounts']))
        log.debug('data schema={0}'.format(db['schema']))


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


    def runQMID(self, qmid):
        '''Run chi2 for an i2b2 query master id'''
        self.qmid = qmid
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

            sql = '''
                select distinct patient_num
                from {0}.qt_patient_set_collection
                where result_instance_id = {1}
            '''.format(self.schema, qdata['result_instance_id'])
            cols, rows = do_log_sql(db, sql)
            self.pats = rows
            self.qiid = qdata['query_instance_id']
            self.qrid = qdata['result_instance_id']
            self.chi_name = 'M{0}_I{1}_R{2}'.format(self.qmid, self.qiid, self.qrid)

        return self.runChi()


    def runPSID_p2(self, referencePatSet, testPatSet, asJSON=False, limit=None):
        if referencePatSet == testPatSet:
            status = 'Job canceled, identical patient sets'
            if asJSON:
                jstr = json.dumps({'cols': [], 'rows': [], 'status': status})
            else: 
                jstr = status
        else:
            # do the reference patient set first
            self.resetPS(self.psid1)
            jstr = self.runPSID(referencePatSet, asJSON, limit, None)
            ref = self.chi_name
            # then do the test patient set, using the reference column name
            self.resetPS(self.psid2)
            jstr = self.runPSID(testPatSet, asJSON, limit, ref)
        return jstr

    def resetPS(self, psid):
        self.psid = psid
        self.psid_done = False
        self.qmid = None
        self.qiid = None
        self.qrid = None


    def runPSID(self, psid, asJSON=False, limit=None, ref='TOTAL'):
        '''Run chi2 for an i2b2 patient set id'''
        if limit is not None:
            self.limit = limit
        self.psid = psid
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        host, port, service, user, pw = self.getCrcOpt()
        dbi = self.getOracleDBI(host, port, service, user, pw)
        with dbi() as db:
            # First check if chi2 results exists for patient set already
            try:
                log.debug('Checking if columns already exist for PSID {0}...'.format(psid))
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
                '''.format(owner, table_name, psid)
                cols, rows = do_log_sql(db, sql)
                if len(rows) > 0:
                    self.chi_name = rows[0][0]
                    self.psid_done = True
                    log.info('Using preexisting chi columns for PSID {0}'.format(psid))
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
                '''.format(self.schema, psid)
                cols, rows = do_log_sql(db, sql)
                if len(rows) == 0:
                    str = 'ERROR, patient set (PSID={0}) not found in QT tables'.format(psid)
                    #log.error(str)
                    return str
                qdata = dict(zip([c.lower() for c in cols], list(rows[0])))
                log.debug('qdata={0}'.format(qdata))
                self.qmid = qdata['query_master_id']
                self.qiid = qdata['query_instance_id']
                self.qrid = qdata['result_instance_id']
                self.chi_name = 'M{0}_I{1}_R{2}'.format(self.qmid, self.qiid, self.qrid)
            else:
                match = re.match('M(?P<psid>\d+)_I(?P<qiid>\d+)_R(?P<qrid>\d+)', self.chi_name)
                self.qmid = match.group('psid')
                self.qiid = match.group('qiid')
                self.qrid = match.group('qrid')

            sql = '''
                select distinct patient_num
                from {0}.qt_patient_set_collection
                where result_instance_id = {1}
            '''.format(self.schema, self.qrid)
            cols, rows = do_log_sql(db, sql)
            self.pats = rows

            return self.runChi(asJSON, ref)


    def runChi(self, asJSON=False, ref='TOTAL'):
        pats = self.pats
        schema = self.schema
        pconcepts = self.pconcepts
        pcounts = self.pcounts
        outfile = self.outfile
        host, port, service, user, pw, temp_table = self.getChiOpt()
        chi_dbi = self.getOracleDBI(host, port, service, user, pw, temp_table)
        with chi_dbi() as db:
            # check if pconcepts exists
            try:
                log.debug('Checking if chi_pconcepts table exists...')
                cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(pconcepts))
            except:
                log.info('chi_pconcepts table ({0}) does not exist, creating it...'.format(pconcepts))
                sql = '''
                create table {0} as
                with obs as (
                    select distinct patient_num pn, concept_cd ccd
                    from {1}.observation_fact
                )
                -- patients who have at least some visit info, on which we will filter using a join
                -- open question: are there any patients missing this code that nevertheless have EMR
                -- data other than demographics?
                , good as (
                    select distinct patient_num
                    from {1}.observation_fact
                    where concept_cd = 'KUMC|DischargeDisposition:0'
                )
                select obs.* from obs join good on pn = patient_num
                '''.format(pconcepts, schema)
                cols, rows = do_log_sql(db, sql)

            # create pcounts table if needed
            try:
                log.debug('Checking if chi_pcounts table exists...')
                cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(pcounts))
            except:
                log.info('chi_pcounts table ({0}) does not exist, creating it...'.format(pcounts))
                sql = '''
                create table {0} as
                select ccd
                , count(distinct pn) total
                , count(distinct pn) / (select count(distinct pn) from {1}) frc_total
                from {1} group by ccd
                union all
                select 'TOTAL' ccd
                , (select count(distinct pn) from {1}) total
                , 1 frc_total from dual
                '''.format(pcounts, pconcepts)
                cols, rows = do_log_sql(db, sql)

            runChi = True
            if self.qmid is not None:
                col_name = self.checkRerunQMID(db)
                if col_name != '':
                    self.chi_name = col_name # already done, but col name may differ
                    runChi = False
            elif self.psid is not None and self.psid_done:
                runChi = False              # already done
            chi_name = self.chi_name

            if runChi:
                # make a temp table of patient set for query chi_name=m###_r###_i###
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

                sql = '''
                update (
                    with cnts as (
                        -- select cohort of interest from {0} table
                        select ccd      -- concept code
                        -- try sometime pc.pn and see if difference
                        , count(distinct mc.pn) cnt  -- count
                        , count(distinct mc.pn) / {3} frc
                                        -- fraction of all patients
                        from {0} pc 
                        join {1} mc on mc.pn = pc.pn 
                        group by ccd
                    )
                    select 
                        pc.{1} emptycnt -- empty target column for counts
                        , nvl(cnts.cnt,0) newcnt -- source column for counts
                        , pc.frc_{1} emptyfrc -- empty target column for fractions
                        , nvl(cnts.frc,0) newfrc -- source column for fractions
                    from {2} pc 
                    left join cnts on pc.ccd = cnts.ccd
                ) up
                set up.emptycnt = up.newcnt, up.emptyfrc = up.newfrc
                '''.format(pconcepts, chi_name, pcounts, len(pats))
                cols, rows = do_log_sql(db, sql)

                sql = '''
                update {0} set {1} = {2}
                , frc_{1} = 1
                where ccd = 'TOTAL'
                '''.format(pcounts, chi_name, len(pats))
                cols, rows = do_log_sql(db, sql)

                cols, rows = do_log_sql(db, 'commit')
                cols, rows = do_log_sql(db, 'drop table {0}'.format(chi_name))

                if asJSON:
                    sql = 'select {0}, {1} from {2}'.format(chi_name, 'frc_%s' % chi_name, pcounts)
                    cols, rows = do_log_sql(db, sql)

            fout = None
            if self.to_file:
                fout = outfile
            if ref != None:
                resp = self.chi2_output(db, chi_name, ref, pcounts, schema, fout, asJSON)
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
        outfile = self.outfile
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
                #log.debug('error.code={0}'.format(error.code))
                #log.debug('error.message={0}'.format(error.message))
                #log.debug('error.context={0}'.format(error.context))
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


    def chi2_output(self, db, colname, ref, pcounts, schema, outfile=None, asJSON=False):
        sql = '''
        with cohort as (
            select {0} pat_count from {1} where ccd = 'TOTAL'
        )
        , data as (
            select ccd, name
            , {4}
            , frc_{4} 
            , {0}
            , frc_{0}
            , power({0} - (cohort.pat_count * frc_{4}), 2) / (cohort.pat_count * frc_{4}) chisq
            /*
            , case
                when frc_{4} = frc_{0} then
                    0
                when frc_{4} > 0 then
                    power({0} - (cohort.pat_count * frc_{4}), 2) / (cohort.pat_count * frc_{4})
                else
                    null
                end chisq
            */
            --, case when frc_{4} < frc_{0} then 1 else -1 end dir
            , case when frc_{4} = frc_{0} then 0 when frc_{4} < frc_{0} then 1 else -1 end dir
            from {1}
            left join (
                select concept_cd, min(name_char) name
                from {2}.concept_dimension
                group by concept_cd
            ) cd on cd.concept_cd = ccd
            , cohort
            where frc_{4} > 0
            --where frc_{4} > 0 or frc_{0} > 0
        )
        , ranked_data as (
            select data.*
            , row_number() over (order by chisq*dir desc) as rank
            , row_number() over (order by chisq*dir asc) as revrank    
            from data   
            where ccd != 'TOTAL'
            order by rank
        ) 
        --select ccd, name, {4}, frc_{4}, {0}, frc_{0}, chisq, dir
        select ccd, name, {4}, frc_{4}, {0}, frc_{0}, (select stddev(chisq) from data) chisq, (select variance(chisq) from data) dir
        from data where ccd = 'TOTAL'
        union all 
        select ccd, name, {4}, frc_{4}, {0}, frc_{0}, chisq, dir
        from ranked_data where rank <= {3} or revrank <= {3}
        '''.format(colname, pcounts, schema, self.limit, ref)
        cols, rows = do_log_sql(db, sql)

        if outfile is not None:
            quote = ['CCD', 'NAME']
            with open(outfile, 'w') as file:
                file.write('%s\n' % ','.join(['\"{0}\"'.format(c) for c in cols]))
                for row in rows:
                    data = dict(zip(cols, row))
                    for k, v in sorted(data.items(), key=lambda x: cols.index(x[0])):
                        if k in quote:
                            file.write('\"{0}\"'.format(v))
                        else:
                            file.write('{0}'.format(v))
                        if k == cols[-1]:
                            file.write('\n')
                        else:
                            file.write(',')

        status = 'Done, chi success!'
        if asJSON:
            jstr = json.dumps({'cols': cols, 'rows': rows, 'status': status})
            return jstr
        return status


def do_log_sql(cur, sql, params=[]): 
    '''Execute sql on given connection and log it
    '''
    cols, rows = None, None
    if len(params) > 1:
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
        log.info(Chi2(args).runPSID(args['-p']))
    elif args['-m']:
        log.info(Chi2(args).runQMID(args['-m']))
    elif args['-t'] and args['-r']:
        log.info(Chi2(args).runPSID_p2(args['-r'], args['-t']))

