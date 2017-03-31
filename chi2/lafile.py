'''ocap_file -- least-privilege interaction with the filesystem
Written by DanC of KUMC apparently but not available via standard
distribution channels, so, welcome to CIRD's repo.

Inspired by:

  The Sash file object is quite similar to (though different from) the
  E file object, which has proven in practice to supply simple,
  intuitive, pola-disciplined interaction with the file system::

    type readable = {
         isDir : unit -> bool;
         exists : unit -> bool;
         subRdFiles : unit -> readable list;
         subRdFile : string -> readable;
         inChannel : unit -> in_channel;
         getBytes : unit -> string;
         fullPath : unit -> string;
    }


 * `How Emily Tamed the Caml`__
   Stiegler, Marc; Miller, Mark
   HPL-2006-116

__ http://www.hpl.hp.com/techreports/2006/HPL-2006-116.html

'''

from ConfigParser import SafeConfigParser

from encap import ESuite


class Readable(ESuite):
    '''Wrap the python file API in the Emily/E least-authority API.

    os.path.join might not seem to need any authority,
    but its output depends on platform, so it's not a pure function.

    >>> import os
    >>> cwd = Readable('.', os.path, os.listdir, open)
    >>> cwd.isDir()
    True

    >>> x = Readable('/x', os.path, os.listdir, open)
    >>> (x / 'y').fullPath()
    '/x/y'

    Authority only goes "down" in the filesystem:

    >>> cwd.subRdFile('../uncle_file')
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    LookupError: Path [../uncle_file] not subordinate ...

    >>> cwd.subRdFile('/etc/passwd')
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    LookupError: Path [/etc/passwd] not subordinate ...

    '''
    def __new__(cls, path0, os_path, os_listdir, openf):
        path = os_path.abspath(path0)

        def isDir(_):
            return os_path.isdir(path)

        def exists(_):
            return os_path.exists(path)

        def subRdFiles(self):
            return [self.subRdFile(n)
                    for n in os_listdir(path)]

        def subRdFile(_, n):
            there = os_path.normpath(os_path.join(path, n))
            if not there.startswith(path):
                raise LookupError(
                    'Path [%s] not subordinate to [%s]' % (n, path))

            return Readable(there, os_path, os_listdir, openf)

        def inChannel(_):
            return openf(path)

        def getBytes(_):
            return openf(path).read()

        def fullPath(_):
            return os_path.abspath(path)

        return cls.make(isDir, exists, subRdFiles, subRdFile, inChannel,
                        getBytes, fullPath,
                        __div__=subRdFile,
                        __trueDiv=subRdFile)


class ListReadable(ESuite):
    '''Simulate a readable directory using a list of pathnames.

    Authorizing access to files listed on the command line is a
    typical use, where any file in the filesystem is fair game:
    >>> import os
    >>> fs = Readable('/', os.path, os.listdir, open)

    @param paths: a list of authorized paths
    @param base: base readable
    @param abspath: given a possibly relative authorized path,
                    return its full path.

    >>> argv = ['prog', 'f1', '/tmp/f2']
    >>> arg_dir = ListReadable(argv[1:], fs, os.path.abspath)

    The result works like a directory with an entry for each of the
    given paths:
    >>> arg_dir.subRdFile('/tmp/f2').fullPath()
    '/tmp/f2'

    No other paths are authorized:
    >>> arg_dir.subRdFile('cheat')
    Traceback (most recent call last):
      ...
    IOError: not an authorized pathname: cheat

    The `_subRdFile` implementation we gave interprets relative pathnames:
    >>> arg_dir.subRdFile('f1').fullPath().startswith(os.getcwd())
    True

    Names must match literally, not just semantically:
    >>> arg_dir.subRdFile('./f1')
    Traceback (most recent call last):
      ...
    IOError: not an authorized pathname: ./f1

    '''

    def __new__(cls, paths, base, abspath):
        paths = paths[:]  # defensive copy, since python lacks immutable lists.

        def isDir(_):
            return True

        def exists(_):
            return True

        def subRdFiles(self):
            return [self.subRdFile(n) for n in paths]

        def subRdFile(self, n):
            if n not in paths:
                raise IOError('not an authorized pathname: %s' % n)
            return base.subRdFile(abspath(n))

        def inChannel(_):
            raise IOError('cannot read directory')

        def getBytes(_):
            raise IOError('cannot read directory')

        def fullPath(_):
            return abspath('')

        return cls.make(isDir, exists, subRdFiles, subRdFile, inChannel,
                        getBytes, fullPath,
                        __div__=subRdFile,
                        __trueDiv=subRdFile)


class ConfigDir(object):
    @classmethod
    def fromRd(cls, rd, base, defaults=None):
        cp = SafeConfigParser(defaults)
        cp.readfp(rd.inChannel(), rd.fullPath())
        return cls(cp, base)


class ConfigRd(ESuite, ConfigDir):
    '''Treat config parameters as read authorization.

    >>> cp = SafeConfigParser()
    >>> cp.add_section('sqlite_db')
    >>> cp.set('sqlite_db', 'file', '/var/run/x.db')
    >>> cp.set('sqlite_db', 'main_table', 't1')

    >>> import os
    >>> fs = Readable('/', os.path, os.listdir, open)

    >>> config_dir = ConfigRd(cp, fs)
    >>> config_dir.subRdFiles()
    [ConfigRd(...)]
    >>> (config_dir / 'sqlite_db').subRdFiles()
    [Readable(...), Readable(...)]
    >>> (config_dir / 'sqlite_db' / 'file').fullPath()
    '/var/run/x.db'

    >>> (config_dir / 'sqlite_db').get('main_table')
    't1'

    >>> sorted((config_dir / 'sqlite_db').items())
    [('file', '/var/run/x.db'), ('main_table', 't1')]

    >>> (config_dir / 'oops').exists()
    False
    >>> config_dir / 'sqlite_db' / 'oops'
    Traceback (most recent call last):
      ...
    NoOptionError: No option 'oops' in section: 'sqlite_db'
    '''

    def __new__(cls, cp, base, section=None):
        def get(self, n):
            if section is None:
                raise ValueError('to get an option value, go into a section.')
            return cp.get(section, n)

        def items(self):
            if section is None:
                raise ValueError('to get items, go into a section.')
            return cp.items(section)

        def isDir(_):
            return True

        def exists(_):
            return section is None or cp.has_section(section)

        def subRdFiles(self):
            return ([self / s for s in cp.sections()]
                    if section is None
                    else [self / opt for opt in cp.options(section)])

        def subRdFile(self, n):
            return (ConfigRd(cp, base, n) if section is None
                    else base.subRdFile(cp.get(section, n)))

        def inChannel(_):
            raise IOError('cannot read directory')

        def getBytes(_):
            raise IOError('cannot read directory')

        def fullPath(_):
            return base.fullPath()

        return cls.make(get, items,
                        isDir, exists, subRdFiles, subRdFile, inChannel,
                        getBytes, fullPath,
                        __div__=subRdFile,
                        __trueDiv=subRdFile)


class Editable(ESuite):
    '''
    >>> import os
    >>> x = Editable('/x', os, open)
    >>> (x / 'y').ro().fullPath()
    '/x/y'

    '''
    def __new__(cls, path, os, openf):
        def _openrd(p):
            return openf(p, 'r')
        _ro = Readable(path, os.path, os.listdir, _openrd)

        def ro(_):
            return _ro

        def subEdFiles(self):
            return [self.subEdFile(n) for n in os.listdir(path)]

        def subEdFile(_, n):
            there = os.path.join(path, n)
            if not there.startswith(path):
                raise LookupError('Path does not lead to a subordinate.')

            return Editable(there, os, openf)

        def outChannel(_):
            return openf(path, 'w')

        def setBytes(self, b):
            outChannel(self).write(b)

        def mkDir(_):
            os.mkdir(path)

        def createNewFile(_):
            setBytes('')

        def delete(_):
            os.remove(path)

        return cls.make(ro, subEdFiles, subEdFile, outChannel,
                        setBytes, mkDir, createNewFile, delete,
                        __div__=subEdFile,
                        __trueDiv=subEdFile)


class ListEditable(ESuite):
    '''a la ListReadable
    '''
    def __new__(cls, paths, base, abspath):
        _ro = ListReadable(paths, base.ro(), abspath)

        def ro(_):
            return _ro

        def subEdFiles(self):
            return [self.subEdFile(n) for n in paths]

        def subEdFile(_, n):
            if n not in paths:
                raise IOError('not an authorized pathname: %s' % n)
            return base.subEdFile(n)

        def outChannel(_):
            raise IOError('cannot write directory')

        def setBytes(_, b):
            raise IOError('cannot write directory')

        def mkDir(_):
            raise IOError('cannot make list directory')

        def createNewFile(_):
            setBytes('')

        def delete(_):
            raise IOError('cannot delete list directory')

        return cls.make(ro, subEdFiles, subEdFile, outChannel,
                        setBytes, mkDir, createNewFile, delete,
                        __div__=subEdFile,
                        __trueDiv=subEdFile)


class ConfigEd(ESuite, ConfigDir):
    '''Treat config parameters as edit (write) authorization.

    >>> ini = """
    ... [sqlite_db]
    ... file: /var/run/x.db
    ... main_table: t1
    ... """

    >>> from StringIO import StringIO
    >>> import os
    >>> ini_rd = Readable('', os.path, os.listdir, lambda n: StringIO(ini))
    >>> fs = Editable('/', os, open)

    >>> config_dir = ConfigEd.fromRd(ini_rd, fs)
    >>> config_dir.subEdFiles()
    [ConfigEd(...)]
    >>> (config_dir / 'sqlite_db').subEdFiles()
    [Editable(...), Editable(...)]
    >>> (config_dir / 'sqlite_db' / 'file').ro().fullPath()
    '/var/run/x.db'

    >>> (config_dir / 'oops').ro().exists()
    False

    NOTE: This is a little buggy; it should return a readable with
    exists() = False.
    >>> config_dir / 'sqlite_db' / 'oops'
    Traceback (most recent call last):
      ...
    NoOptionError: No option 'oops' in section: 'sqlite_db'
    '''

    def __new__(cls, cp, base, section=None):
        def ro(_):
            return ConfigRd(cp, base.ro(), section)

        def subEdFiles(self):
            return ([self / s for s in cp.sections()]
                    if section is None
                    else [self / opt for opt in cp.options(section)])

        def subEdFile(self, n):
            return (ConfigEd(cp, base, n) if section is None
                    else base.subEdFile(cp.get(section, n)))

        def outChannel(_):
            raise IOError()

        def setBytes(_):
            raise IOError()

        def mkDir(_):
            raise IOError('cannot make config directory')

        def createNewFile(_):
            setBytes('')

        def delete(_):
            raise IOError('cannot delete config directory')

        return cls.make(ro, subEdFiles, subEdFile, outChannel,
                        setBytes, mkDir, createNewFile, delete,
                        __div__=subEdFile,
                        __trueDiv=subEdFile)


def walk_ed(top):
    '''ocap analog to os.walk for editables
    '''
    for x in _walk(top, lambda ed: ed.subEdFiles(),
                   ro=lambda ed: ed.ro()):
        yield x


def walk_rd(top):
    '''ocap analog to os.walk
    '''
    for x in _walk(top, lambda rd: rd.subRdFiles()):
        yield x


def _walk(top, sub_files, ro=lambda rd: rd):
    '''ocap analog to os.walk
    '''
    subs = [(sub, ro(sub).isDir())
            for sub in sub_files(top)]
    dirs = [s for (s, d) in subs if d]
    nondirs = [s for (s, d) in subs if not d]

    yield top, dirs, nondirs

    for subd in dirs:
        for x in _walk(subd, sub_files, ro):
            yield x


def relName(ed, anc):
    '''Get the name of an Editable relative to an ancestor.
    '''
    apath = anc.ro().fullPath()
    epath = ed.ro().fullPath()
    assert(epath.startswith(apath))
    return epath[len(apath) + 1:]


def relName_rd(rd, anc):
    '''Get the name of a Readable relative to an ancestor.
    '''
    apath = anc.fullPath()
    path = rd.fullPath()
    assert(path.startswith(apath))
    return path[len(apath) + 1:]
