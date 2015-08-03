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
    opt['database']['qmid'] = arguments['QMID']
    log.debug('opt:\n%s' % opt)
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
    log.debug('chi_schema={0}'.format(db['chi_schema']))

def main(opt):
    db=opt['database']
    debug_dbopt(db)
    host, port, user = db['crc_host'], db['crc_port'], db['crc_user']
    service, schema, qmid = db['crc_service_name'], db['schema'], db['qmid']
    chi_host, chi_port, chi_user = db['chi_host'], db['chi_port'], db['chi_user']
    chi_service = db['chi_service_name']
    pconcepts, pcounts,pschema = db['chi_pconcepts'], db['chi_pcounts'], db['chi_schema']
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
            where qm.query_master_id={1} and rownum = 1
            order by ps.result_instance_id desc, qi.query_instance_id desc
        '''.format(schema, qmid)
        cols, rows = do_log_sql(db, sql)
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
        chi_name = 'm{0}_i{1}_r{2}'.format(
            qdata['query_master_id'], qdata['query_instance_id'], 
            qdata['result_instance_id'])
        log.info('chi_pcounts={0}'.format(pcounts))
        log.info('chi_name={0}'.format(chi_name))

    log.debug('keyring get oracle://{0}/{1} {2}'.format(chi_host, chi_service, chi_user))
    log.debug('*******')
    chi_pw = keyring.get_password('oracle://{0}/{1}'.format(chi_host, chi_service), chi_user)
    chi_dsn = cx.makedsn(chi_host, int(chi_port), service_name=chi_service)
    def chiDB(it=[]):
        if not it:
            it.append(cx.connect(chi_user, chi_pw, chi_dsn))
        return it[0]
    chi_dbi = dbmgr(chiDB)

    with chi_dbi() as db:
	# had to separate schema and name of pconcepts so that the check for existance below would work
        cols, rows = do_log_sql(db,"select count(*) from all_objects where object_type = 'TABLE' and object_name = '{0}'".format(pconcepts.upper()))
	# setting pconcepts to what the rest of the code will expect it to be
	pconcepts = pschema+"."+pconcepts
        if rows[0][0] == 0:
            print "creating {0}".format(pconcepts);
            sql = '''
                create table {0} as with 
                obs as (select distinct patient_num pn,concept_cd ccd from {1}.observation_fact),
                -- patients who have at least some visit info, on which we will filter using a join
                good as (select distinct patient_num 
                    from {1}.observation_fact where concept_cd = 'KUMC|DischargeDisposition:0')
                select obs.* from obs join good on pn = patient_num
	    '''.format(pconcepts,schema)
	    cols, rows = do_log_sql(db, sql)

        cols, rows = do_log_sql(db,"select count(*) from all_objects where object_type = 'TABLE' and object_name = '{0}'".format(pcounts.upper()))
	pcounts = pschema+"."+pcounts

	if rows[0][0] == 0:
            print "creating {0}".format(pcounts);
            cols, rows = do_log_sql(db,"select count(distinct pn) from {0}".format(pconcepts))
            tnp = rows[0][0]
            sql = '''
                create table {0} as
                select ccd, count(distinct pn) total, count(distinct pn)/{1} frc_total
                from {2} group by ccd
                union all
                select 'TOTAL' ccd,{1} total, 1 frc_total from dual
	    '''.format(pcounts,tnp,pconcepts)
            cols, rows = do_log_sql(db, sql)

        try:
            cols, rows = do_log_sql(db, 'drop table {0}'.format(chi_name))
        except:
            pass

        # When patient_mapping is empty, script errors out, had to use patient_dimension instead
        sql = '''
            create table {0} as 
                select patient_num pn
                from {1}.patient_dimension
                where 1 = 0
        '''.format(chi_name, schema)
        cols, rows = do_log_sql(db, sql)

        sql='insert into {0} (pn) values (:pn)'.format(chi_name)
        cols, rows = do_log_sql(db, sql, [[p[0]] for p in pats])
        sql = 'alter table {0} add {1} number'.format(pcounts, chi_name)
        import pdb;pdb.set_trace()
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


def dbmgr(connect):
    '''Make a context manager that yields cursors, given connect access.
    '''
    @contextmanager
    def dbtrx():
        conn = connect()
        cur = conn.cursor()
        try:
            yield cur
        except:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            cur.close()
    return dbtrx


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

