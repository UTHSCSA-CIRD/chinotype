#!/user/bin/env python
'''
Create counts and prevalences for ranking patient cohorts
   by relative prevalence of concepts.

Usage:
   driver.py [options] QMID

Options:
    -h --help           Show this screen
    -v --verbose        Verbose/debug output (show all SQL)
    -c --config=FILE    Configuration file [default: config.ini]
    -o --output=FILE    Create an chi2 output file

QMID is the query master ID (from i2b2 QT tables). The latest query 
instance/result for a given QMID will be used.

Configure database users in config file and set password in keyring a la
   $ keyring set oracle://crc_host/crc_service_name crc_user
   $ keyring set oracle://chi_host/chi_service_name chi_user
'''
from sys import argv
from docopt import docopt
from ConfigParser import SafeConfigParser
import cx_Oracle as cx
from contextlib import contextmanager
import keyring
import logging

'''
TODO: take a second, optional QMID, process exactly as first (i.e. if present, reuse)
TODO: when there are two QMIDs, also generate a third patient-set which are all the unique PATIENT_NUMs in the union of the two patient sets
TODO: when there are two QMIDs, use the joint QMID as the reference population for the first QMID (when one QMID use TOTAL as is done now)
TODO: the joint QMID should be inserted just like the individual QMIDs (unless it already exists)
TODO: to facilitate finding existing joint QMIDs regardless of what order their component QMIDs were specified, their names should be a concatenation of the ordered QMID names
TODO: allow either or both QMIDs to be accompanied by an optional alphanumeric shortname (for readability of the output table later on), if missing then use the QMID (or dig out of QT table)
TODO: when inserting columns into PCONCEPT_COUNTS also insert the CHI_NAME, SHORTNAME, and len(pats) into a table, say, CHI_DD; create if missing
TODO: the SHORTNAME of a joint QMID is SHORTNAME1+"_"+SHORTNAME2
TODO: when outputting the CSV file, replace CHI_NAMEs with SHORTNAMEs
TODO: when outputting the CSV file, use WHERE JOINT_QMID > 0 (i.e. omit rows where the reference population and sub-population of interest have 0 counts)
TODO: possibly look up certain things from CHI_DD instead of recalculating/searching each time? Either way, definitely needed for CSV headers
'''

log = logging.getLogger(__name__)

def config():
    logging.basicConfig(format='%(asctime)s: %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S', level=logging.INFO)
    arguments = docopt(__doc__, argv=argv[1:])
    if arguments['--verbose']:
        log.setLevel(logging.DEBUG)
    config_fn = arguments['--config']
    cp = SafeConfigParser()
    cp.readfp(open(config_fn, 'r'), filename=config_fn)
    opt = cp._sections
    # Can be simplified
    #if arguments['--output']:
    #    opt['output_file'] = True    
    #else: 
    #    opt['output_file'] = False
    opt['output'] = arguments['--output']
    opt['database']['qmid'] = arguments['QMID']
    log.debug('opt:\n{0}'.format(opt))
    return opt

def debug_dbopt(db):
    log.debug('host={0}'.format(db['crc_host']))
    log.debug('port={0}'.format(db['crc_port']))
    log.debug('service={0}'.format(db['crc_service_name']))
    log.debug('user={0}'.format(db['crc_user']))
    log.debug('schema={0}'.format(db['schema']))
    log.debug('chi_host={0}'.format(db['chi_host']))
    log.debug('chi_port={0}'.format(db['chi_port']))
    log.debug('chi_service={0}'.format(db['chi_service_name']))
    log.debug('chi_user={0}'.format(db['chi_user']))
    log.debug('chi_pconcepts={0}'.format(db['chi_pconcepts']))
    log.debug('chi_pcounts={0}'.format(db['chi_pcounts']))

def main(opt):
    db=opt['database']
    debug_dbopt(db)
    if opt['output']:
        csvfile = opt['output']
        log.info('output={0}'.format(csvfile))
    #csvfile = (opt['output']['csv'])
    #if opt['output_file']:
    #    log.info('output={0}'.format(csvfile))
    host, port, user = db['crc_host'], db['crc_port'], db['crc_user']
    service, schema, qmid = db['crc_service_name'], db['schema'], db['qmid']
    chi_host, chi_port, chi_user = db['chi_host'], db['chi_port'], db['chi_user']
    chi_service = db['chi_service_name']
    pconcepts, pcounts = db['chi_pconcepts'], db['chi_pcounts']

    log.debug('keyring get oracle://{0}/{1} {2}'.format(host, service, user))
    log.debug('*******')
    pw = keyring.get_password('oracle://{0}/{1}'.format(host, service), user)
    dsn = cx.makedsn(host, int(port), service_name=service)
    def crcDB(it=[]):
        if not it:
            it.append(cx.connect(user, pw, dsn))
        return it[0]
    dbi = dbmgr(crcDB)

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
        '''.format(schema, qmid)
        cols, rows = do_log_sql(db, sql)
        if len(rows) == 0:
            log.error('ERROR, QMID {0} has no patient set result instance'.format(qmid))
            raise SystemExit
        qdata = dict(zip([c.lower() for c in cols], list(rows[0])))
        log.debug('qdata={0}'.format(qdata))

        sql = '''
            select distinct patient_num
            from {0}.qt_patient_set_collection
            where result_instance_id = {1}
        '''.format(schema, qdata['result_instance_id'])
        cols, rows = do_log_sql(db, sql)
        pats = rows
        log.info('patient count={0}'.format(len(pats)))
        chi_name = 'M{0}_I{1}_R{2}'.format(
            qdata['query_master_id'], qdata['query_instance_id'], 
            qdata['result_instance_id'])
        log.info('chi_pconcepts={0}'.format(pconcepts))
        log.info('chi_pcounts={0}'.format(pcounts))

    log.debug('keyring get oracle://{0}/{1} {2}'.format(chi_host, chi_service, chi_user))
    log.debug('*******')
    chi_pw = keyring.get_password('oracle://{0}/{1}'.format(chi_host, chi_service), chi_user)
    chi_dsn = cx.makedsn(chi_host, int(chi_port), service_name=chi_service)
    def chiDB(it=[]):
        if not it:
            it.append(cx.connect(chi_user, chi_pw, chi_dsn))
        return it[0]
    chi_dbi = dbmgr(chiDB, chi_name)

    with chi_dbi() as db:
        # check if pconcepts exists
        try:
            log.debug('Checking if chi_pconcepts table exists...')
            cols, rows = do_log_sql(db, 'select 1 from {0} where rownum = 1'.format(pconcepts))
        except:
            #log.error('ERROR, chi_pconcepts table does not exist')
            #raise SystemExit
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

        # if pcounts has QMID & patient count matches latest, return existing results
        # if pcounts has QMID & patient count DOES NOT match latest, warn/exit
        try:
            log.debug('Checking if columns already exist for QMID {0}...'.format(qmid))
            table_info = pcounts.split('.')
            owner, table_name = '', ''
            if len(table_info) > 1:
                owner = 'and owner = \'{0}\''.format(table_info[0].upper())
            if len(table_info) > 0:
                table_name = 'and table_name = \'{0}\''.format(table_info[1].upper())
            # note new fuzzy match below-- matched FOO and frc_FOO
            sql = '''
            select column_name from all_tab_columns
            where 1=1 {0} {1}
            and column_name like '%M{2}_%'
            '''.format(owner, table_name, qmid)
            cols, rows = do_log_sql(db, sql)
            # if already do exist
            # note that it should really be == 2, otherwise permits error condition where
            # just FOO or frc_FOO exists, but script thinks both do
            if len(rows) == 2:
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
                    log.info('chi_name={0}'.format(column_name))
                    #if opt['output_file']:
                    if csvfile:
                        output_chi_table(db, column_name, csvfile, pcounts, schema)
                else:
                    # This should never happen unless i2b2 QT table are corrupt 
                    # or out of sync with the chi_pcounts table
                    log.info('ERROR, columns exist for QMID {0} but patiet set differs'.format(qmid))
                    log.info('  new_query: {0}'.format(chi_name))
                    log.info('  old_query: {0}'.format(column_name))
                raise SystemExit
            elif len(rows) == 0:
                log.info('chi_name={0}'.format(chi_name))
                # Actually, shouldn't we do the rest of this function in here?
                # Cannot do otutpu_chi_table here, because when > 0 (now == 2) fails
                # there is no chi_name yet to query from that table
                # make a temp table of patient set for query chi_name=m###_r###_i###
                log.debug('Creating temp table for patient set...')
                try: 
                    cols, rows = do_log_sql(db,'drop table {0}'.format(chi_name))
                except:
                    pass
                finally:
                    log.debug('No existing table to drop')

                # note below that patient_mapping doesn't always exist, but patient_dimension does
                sql = '''
                create table {0} as 
                    select patient_num pn
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
                import pdb;pdb.set_trace()
                if csvfile:
                    output_chi_table(db, chi_name, csvfile, pcounts, schema)

        except cx.DatabaseError as e:
            # pcounts does not have existing QMID columns
            raise
            #pass


def dbmgr(connect, temp_table=None):
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


def output_chi_table(db, colname, csvfile, pcounts, schema):
    sql = '''
    with cohort as (
        select {0} pat_count from {1} where ccd = 'TOTAL'
    )
    select ccd, name
    , total
    , frc_total
    , {0}
    , frc_{0}
    , power(cohort.pat_count * frc_total - {0}, 2) / (cohort.pat_count * frc_total) chisq
    , case when frc_total < frc_{0} then 1 else -1 end dir
    from {1}
    join (
        select concept_cd, min(name_char) name
        from {2}.concept_dimension
        group by concept_cd
    ) cd on cd.concept_cd = ccd
    , cohort
    order by chisq*dir desc
    '''.format(colname, pcounts, schema)
    cols, rows = do_log_sql(db, sql)

    quote = ['CCD', 'NAME']
    with open(csvfile, 'w') as file:
        file.write('%s\n' % ','.join(['\"{0}\"'.format(c) for c in cols]))
        # We'll need all of them, and definitely not just the *top* 100
        #for row in rows[0:100]:
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
    opt = config()
    main(opt)

