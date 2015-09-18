'''param_check -- Queue builder jobs after checking credentials, parameters
---------------------------------------------------------------------------

.. note:: See also :ref:`well-typed` regarding type declarations,
          `scala_hide`, `classOf`, and `pf_`.
.. i.e. devdoc/well_typed.rst

.. note:: @@I'm using spaces at the end of the line, which get flagged
          by pep8 and show up red in my emacs, to highlight TODOs.

'''

import json
import logging
from functools import partial as pf_

import argh
from paste.request import parse_formvars
from ocap import lafile
from chinotype import Chi2

import i2b2hive

log = logging.getLogger(__name__)


def cgi_main(argv, arg_wr, clock,
             mkCGIHandler, mkBrowser,
             queue_dir='queue',
             log_name='chi2.log'):


    [hive_addr, pm_addr, request_log_dir] = argv[1:4]
    log_wr = arg_wr / request_log_dir
    queue_wr = log_wr / queue_dir
    log_request = mk_log_request(log_wr, clock, 'logging')
    queue_request = mk_log_request(queue_wr, clock, 'queueing')
    browser = mkBrowser()
    account_check = i2b2hive.AccountCheck(hive_addr, pm_addr, browser)

    job_setup = JobSetUp(account_check, queue_request)
    app = WellFormedPost(job_setup, JobSetUp.mandatory_params, log_request)

    cgi = mkCGIHandler(
        log_wr / log_name,
        level=logging.DEBUG if '--debug' in argv else logging.INFO)
    cgi.run(app)


#TODO: import from elsewhere to keep it from obscuring JobSetUp
def decode_concepts(txt):
    concepts = json.loads(txt)
    for n in ['keys', 'names']:
        if n not in concepts:
            raise ValueError('expected in concepts: ' + n)
        if not isinstance(concepts[n], type([])):
            raise ValueError('expected [...] for ' + n)
    return concepts


class JobSetUp(object):
    mandatory_params = [('pgsize', int),
                        ('patient_set_1', int),
                        ('patient_set_2', int)]

    def __init__(self, account_check, queue_request,
                 out_key='str'):
        '''JobSetUp constructor

        :type account_check: i2b2pm.AccountCheck
        :param queue_request: access to queue requests
        :param String out_key: object key where HTTP client
                               expects to find job summary
        '''
        '''
        def queue(username, filename, **job_info):
            queue_request(username, dict(job_info,
                                         filename=filename,
                                         username=username))
            log.info('job queued for %s: %s', username, filename)
            return { out_key: 'job queued for %s: %s' % (username, filename)}

        self.queue_if_authz = account_check.restrict(lambda *args: queue)
        '''
        def do_job(username, patient_set_1, patient_set_2, pgsize, **job_info):
            log.info('running job for user=%s, patient_set_1=%s, patient_set_2=%s', \
                username, patient_set_1, patient_set_2)
            if patient_set_1 == 0:
                chistr = Chi2().runPSID(patient_set_2, True, pgsize)
            else:
                chistr = Chi2().runPSID_p2(patient_set_1, patient_set_2, True, pgsize)
            chijson = json.loads(chistr)
            log.info('response=%s', chijson['status'])
            return { out_key: chistr }
 
        self.do_if_authz = account_check.restrict(lambda *args: do_job)

    def __call__(self, env, start_response,
                 username, password,
                 pgsize, patient_set_1, patient_set_2):
        '''Handle HTTP request per `WSGI`__.

        __ http://www.python.org/dev/peps/pep-0333/

        :param env: access to HTTP request
        :type env: Dict[String, String]
        :param start_response: access to start HTTP response
        :type start_response: (String, Seq[(String, String)]) => Unit

        :param String pgsize: page size, # concepts to display (numeral)
        :param String patient_set_1: patient_set id (numeral)
        :param String patient_set_2: patient_set id (numeral)

        :rtype: Iterable[String]
        '''

        log.info('checking i2b2 password for: %s', username)
        try:
            password = i2b2hive.pw_decode(password)
            #queue = self.queue_if_authz((username, password))
            do_job = self.do_if_authz((username, password))
        except (i2b2hive.HiveError, ValueError) as ex:
            raise NotAuthorized(ex)

        log.debug('i2b2 credentials OK for %s', username)
        out = do_job(username, patient_set_1, patient_set_2, pgsize)

        start_response('200 OK',
                       [('content-type', 'application/json')])
        return [json.dumps(out)]


class ClientError(IOError):
    '''HTTP 4xx errors
    '''
    pass


class NotAuthorized(ClientError):
    pass


class WellFormedPost(object):
    r'''WSGI app for ensuring POST method and parameters
    and handling errors and logging.

    Let's put the main application logic in something like a WSGI app,
    but it can assume the username, password, and other parameters
    (e.g. `knob`) are already parsed:

      >>> def my_app(env, start_response,
      ...            username, password, label, knob):
      ...     if password != 'lemmein':
      ...         raise NotAuthorized
      ...     start_response('200 OK', [])
      ...     return [label, ': ', str(knob + 1)]

    And let's suppose we have a way of logging the (alleged) username
    and the parameters of a request:

      >>> logstore = []
      >>> log1 = lambda who, params: logstore.append((who, params))

    Now we can make a WSGI app that checks the parameters and calls
    the application logic. Note that each parameter may have an associated
    decoding function to, for example, to convert to int:

      >>> check_post = WellFormedPost(my_app,
      ...     [('label', None), ('knob', int)], log1)

      >>> from webtest import TestApp
      >>> tapp = TestApp(check_post)

    A well-formed request is a POST with (at least) `username`,
    `password`, and the other mandatory parameters:

      >>> tapp.post('/', params=dict(
      ...     username='me', password='lemmein',
      ...     knob='3', label='X'))
      <200 OK text/html body='X: 4'>

    We can see that the request got logged::

      >>> logstore
      [('me', {'knob': '3', 'label': 'X'})]

    The application logic can signal an auth failure:
      >>> tapp.post('/', params=dict(
      ...     username='me', password='password',
      ...     knob='3', label='X'))
      ... # doctest: +ELLIPSIS
      Traceback (most recent call last):
        ...
      AppError: Bad response: 403 not authorized ...
      'incorrect credentials'

    Other modes include ill-formed parameters:

      >>> tapp.post('/', params=dict(
      ...     username='me', password='lemmein',
      ...     knob='high', label='X'))
      ... # doctest: +ELLIPSIS
      Traceback (most recent call last):
        ...
      AppError: Bad response: 400 bad request ...
      "Incorrect parameters:invalid literal for int() with base 10: 'high'"

    and missing parameters:

      >>> tapp.post('/', params=dict(
      ...     username='me', password='lemmein',
      ...     label='X'))
      ... # doctest: +ELLIPSIS
      Traceback (most recent call last):
        ...
      AppError: Bad response: 400 bad request ...
      'Incorrect parameters:"\'knob\'"'

    '''

    mandatory_params = ['username', 'password']

    def __init__(self, subApp, sub_params, log_request):
        '''
        :param log_request: access to log authorized requests
        :type log_request: (String, Dict[String, String]) => Unit
        '''
        self._subApp = subApp
        self._log_request = log_request
        self._sub_params = sub_params

    def __call__(self, env, start_response):
        '''Handle HTTP request per `WSGI`__.

        __ http://www.python.org/dev/peps/pep-0333/

        :param env: access to HTTP request
        :type env: Dict[String, String]
        :param start_response: access to start HTTP response
        :type start_response: (String, Seq[(String, String)]) => Unit
        :rtype: Iterable[String]

        .. note:: We ignore extra HTTP request parameters.
        '''
        if env['REQUEST_METHOD'] != 'POST':
            start_response('405 method not allowed',
                           [('content-type', 'text/plain')])
            log.error('app called with non-POST method: %s',
                      env['REQUEST_METHOD'])
            return ['Bzzt. We only do POST.']

        args = parse_formvars(env)

        identity = lambda x: x

        try:
            [username, password] = [
                args.pop(k) for k in self.mandatory_params]
            log.info('Request from %s.', username)
            mvalues = dict([
                (k, (txform or identity)(args[k]))
                for (k, txform) in self._sub_params])
        except (KeyError, ValueError) as ex:
            start_response('400 bad request',
                           [('content-type', 'text/plain')])
            log.error('incorrect credentials for %s', username)
            return ['Incorrect parameters:', str(ex)]

        self._log_request(username, dict(args))
        try:
	    #log.info('env=%s', env)
	    #log.info('start_response=%s', start_response)
	    #log.info('username=%s', username)
	    #log.info('password=%s', password)
	    #log.info('mvalues=%s', mvalues)
            return self._subApp(env, start_response,
                                username, password,
                                **mvalues)
        except NotAuthorized:
            start_response('403 not authorized',
                           [('content-type', 'text/plain')])
            return ['incorrect credentials']
        # For debugging, catch IOError instead
        except Exception, ex:
            log.critical('Error:', exc_info=ex)
            start_response('500 I tried.',
                           [('content-type', 'text/plain')])
            return ['error:', str(ex)]


def mk_log_request(log_dir, clock, event_label):
    '''
    :param lafile.Editable log_dir: access to write log files
    :param clock: access to date and time of day
    :type clock: () => datetime
    '''
    def log_request(username, params):
        '''
        :param String username: who is accountable for the request
        :param Dict[String,String] params: request parameters
        '''
        timestamp = str(clock()).replace(' ', 'T')  # space in filenames are a PITA
        request_log_file = '%s-%s.json' % (timestamp, username)
        log.info('%s request parameters to: %s', event_label, request_log_file)
        with (log_dir / request_log_file).outChannel() as out:
            json.dump(params, out)

    return pf_(log_request)


def mk_access(os, openf, argv):
    '''@@doc''' 

    # TODO: mock os, openf, argv
    # rather than mock_config so that we can unit test
    # this function.

    write_any_file = lafile.Editable('/', os, openf)
    arg_wr = lafile.ListEditable(argv, write_any_file,
                                 os.path.abspath)

    return arg_wr


class MockClock(object):
    def __init__(self):
        from datetime import datetime, timedelta
        self._now = datetime(2001, 01, 01)
        self._delta = timedelta(seconds=30)

    def now(self):
        self._now += self._delta
        return self._now


class MockCGIHandler(object):
    def __init__(self, params,
                 addr='/'):
        def run(app):
            from webtest import TestApp
            tapp = TestApp(app)
            tapp.post(addr, params=params)
        self.run = run


if __name__ == '__main__':
    def _trusted_main():
        from sys import argv
        from __builtin__ import open as openf
        import os
        from datetime import datetime
        from wsgiref.handlers import CGIHandler

        from mechanize import Browser

        def mkCGIHandler(log_wr,
                         level=logging.INFO):
            logging.basicConfig(
                level=level,
                format='%(asctime)s %(levelname)s %(name)s %(message)s',
                filename=log_wr.ro().fullPath())
            return CGIHandler()

        arg_wr = mk_access(os, openf, argv)

        if 'SCRIPT_NAME' in os.environ:  # Running as CGI
            cgi_main(argv, arg_wr,
                     mkCGIHandler=mkCGIHandler,
                     clock=datetime.now,
                     mkBrowser=lambda: Browser())
                       
        else:  # We're running from the command line
            raise NotImplementedError('No CLI usage. CGI only.')

    _trusted_main()
