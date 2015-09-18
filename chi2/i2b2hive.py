'''i2b2hive -- i2b2 hive/cell client API
........................................

Integration test usage::

  $ python i2b2hive.py http:.../index.php http:.../PMService/ demo demouser
  ...
  INFO:__main__:projects: ['BlueHeron']

.. note:: See also :ref:`well-typed` regarding type declarations.

'''

import logging
import pkg_resources
import xml.etree.ElementTree as ET

# We use the Browser interface (type) at the module level,
# but instantiating it is a capability limited to trusted code.
from mechanize import Browser

from ocap import lafile

log = logging.getLogger(__name__)

KEY1 = r'\\i2b2\i2b2\Diagnoses\Neoplasms (140-239)' '\\'


def _res(fn):
    ''':type fn: String'''
    return pkg_resources.resource_string(__name__, fn)

MSG_get_user_configuration = _res('msgs/i2b2_get_user_config.xml')
MSG_PDO = _res('msgs/i2b2_get_pdo.xml')


def _integration_test_main(argv,
                           mkBrowser,
                           max_patients=200):
    '''
    :param Seq[String] argv: CLI args
    :param mkBrowser: web access factory
    :type mkBrowser: () => Browser
    '''
    [hive_addr, pm_addr, username, password_markup] = argv[1:]

    session_key = pw_decode(password_markup)

    def only_if_creds_ok(username, sk, cells, projects):
        log.debug("session key: %s", sk)
        log.info("cells: %s", cells)
        log.info("projects: %s", projects)

    acct_check = AccountCheck(hive_addr, pm_addr, mkBrowser())
    checked_access = acct_check.restrict(only_if_creds_ok)
    checked_access((username, session_key))


class HiveUA(object):
    def __init__(self, hive_addr, browser):
        '''
        :param String hive_addr: web address of i2b2 hive endpoint
                                 usually `http://.../index.php`
        :param Browser browser: web access a la mechanize
        '''
        # Skip robots.txt
        browser.set_handle_robots(False)

        def send_request(bodyq):
            log.debug('request body: %s', bodyq)
            browser.method = "POST"
            browser.addheaders = [('Content-Type', 'text/xml')]
            reply = browser.open(hive_addr, bodyq)
            body = reply.read()
            log.debug('reply: %s...', body[:40])
            return body
        self.send_request = send_request

    def _post_to_hive(self, redirect, request_template, parts):
        '''Fill in template and send request.

        :param String redirect: destination of request
        :param String request_template: cf. msgs/ directory
        :type parts: Dict[String, String]
        '''
        log.debug('_post_to_hive %s: %s ...', redirect,
                  request_template[:40])
        for n, v in parts.items():
            log.debug('part %s: %s', n, v)
        bodyq = request_template % dict(parts, REDIRECT=redirect)
        body = self.send_request(bodyq)
        body_parsed = ET.fromstring(body)
        log.debug('_post_to_hive parsed reply: %s', body_parsed)
        return body_parsed


class BadFormat(ValueError):
    '''Message (e.g. from PM cell) has bad format.
    '''
    pass


class HiveError(IOError):
    '''ERROR status in hive message response
    '''
    pass


class AccountCheck(HiveUA):
    '''Require i2b2 PM account to invoke access.
    '''
    def __init__(self, hive_addr, urlCellPM, browser):
        '''

        .. note:: In :ref:`well-typed python <well-typed>`,
                  constructors with args can't (yet?) be implicitly
                  inherited.

        :param String hive_addr: address of i2b2 hive endpoint (index.php)
        :param String urlCellPM: address of i2b2 PMService,
                                 as in i2b2_config_data.js
        :param Browser browser: web access
        :param doit: function to be called only if the PM cell says OK
        '''
        HiveUA.__init__(self, hive_addr, browser)
        self.urlCellPM = urlCellPM

    def restrict(self, access):
        '''Make a restricted form of an access function.

        :type access: (String, String, Dict[String, String], Seq[String]) => U
        :return: a function from (username, password) to what accss returns
        :rtype: ((String, String)) => U

        :forall: U
        '''
        def restricted(authz):
            session_key, cells, projects = self.get_user_configuration(authz)
            username, password = authz
            return access(username, session_key, cells, projects)
        return restricted

    def get_user_configuration(self, authz,
                               path='getServices'):
        '''Check credentials and get user's projects etc.

        .. note:: `get_...` seems misleading; this is stateful.

        '''
        username, password = authz
        doc = self._post_to_hive(self.urlCellPM + path,
                                 MSG_get_user_configuration,
                                 dict(USERNAME=username,
                                      PASSWORD=password))
        return AccountCheck._parse_config(doc, username)

    @classmethod
    def _parse_config(cls, doc, username):
        '''
        Handle errors::

          >>> doc = ET.fromstring('<garbage/>')
          >>> AccountCheck._parse_config(doc, 'anybody')
          Traceback (most recent call last):
            ...
          BadFormat: cannot find username in response

          >>> doc = ET.fromstring(
          ...     '<response><status type="ERROR">oops!</status></response>')
          >>> AccountCheck._parse_config(doc, 'anybody')
          Traceback (most recent call last):
            ...
          HiveError: oops!

        '''
        uelts = doc.findall('.//user')
        log.debug('uelts: %s', uelts)
        if not uelts or username != uelts[0].findall('user_name')[0].text:
            errs = [elt for elt in doc.findall('status')
                    if elt.attrib['type'] == "ERROR"]
            if errs:
                log.debug('errors: %s', [e.text for e in errs])
                raise HiveError(errs[0].text)
            raise BadFormat('cannot find username in response')
        session_key = uelts[0].findall('password')[0].text
        cells = dict([(c.attrib['id'], c.findall('url')[0].text)
                      for c in doc.findall('.//cell_datas')[0]])
        projects = [p.attrib['id']
                    for p in uelts[0].findall('project')]
        return session_key, cells, projects


def pw_decode(txt):
    '''
    :param String txt: i2b2 password element text

    >>> pw_decode("""<password token_ms_timeout="1800000"
    ...     is_token="true">SessionKey:enE9dIrNqdhVAfekaXSe</password>""")
    'SessionKey:enE9dIrNqdhVAfekaXSe'

    >>> pw_decode("""sekret""")
    'sekret'

    '''
    txt = txt[txt.index('>') + 1:] if '>' in txt else txt
    txt = txt[:txt.index('<')] if '<' in txt else txt
    return txt


def ds_access(user_name, rdFiles,
              suffix='-ds.xml'):
    '''Get connection details of a jboss datasource by user-name.

    :param String user_name: JNDI username of desired datasource
    :param rdFiles: iterator of rd's to check

    .. note:: KLUDGE: this is an integration test, not a unit test:

    >>> import os
    >>> crd = lafile.Readable('test_jboss_deploy', os.path, os.listdir, open)

    >>> ds_access('BLUEHERONdata', crd.subRdFiles())
    ('xyzpdq', 'testhost', '1521', 'DB1')

    Note case sensitivity:

    >>> ds_access('BlueHeronData', crd.subRdFiles())
    Traceback (most recent call last):
      ...
    IndexError: BlueHeronData

    >>> ds_access('BLUEHERONdata', (crd / 'does_not_exist').subRdFiles())
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    OSError: [Errno 2] ...

    :raises: XMLSyntaxError on failure to parse XML files therein

    '''
    # Refer to type since pyflakes can't see in docstrings
    classOf(lafile.ConfigRd)

    docs = [ET.fromstring(rd.inChannel().read())
            for rd in rdFiles
            if rd.fullPath().endswith(suffix)]

    srcs = [(src.findall('password'),
             src.findall('connection-url'),
             src.findall('user-name'))
            for doc in docs
            for src in doc.findall('local-tx-datasource')]

    my_srcs = [(pw[0].text, conn[0].text)
               for (pw, conn, u) in srcs
               if (user_name in [e.text for e in u]) and pw and conn]

    if not my_srcs:
        raise IndexError(user_name)

    pw, url = my_srcs[0]
    [host, port, sid] = url.split('@', 1)[1].split(':', 2)
    return pw, host, port, sid


def mock_browser():
    from mechanize import Browser

    class Kludge(Browser):
        def open(self, addr, body=None):
            from StringIO import StringIO
            return StringIO('''
            <reply>
              <user>
                <user_name>knock-knock</user_name>
                <password>session-key:12345</password>
              </user>
              <cell_datas />
            </reply>
            ''')

    return Kludge()


classOf = lambda cls: cls  # scala_hide


if __name__ == '__main__':
    def _configure_logging(level=logging.DEBUG):
        ''':type level: Int'''
        logging.basicConfig(level=level)

    def _with_caps():
        from sys import argv

        _integration_test_main(argv=argv,
                               mkBrowser=lambda: Browser())

    _configure_logging()
    _with_caps()
