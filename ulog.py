import os
import struct


TCULMAGICNUM = 0xc9

TYPE_KV = 'kv'
TYPE_K = 'k'


COMMANDS = {
    0x10: ('put', TYPE_KV), # TTCMDPUT
    0x11: ('putkeep', TYPE_KV), # TTCMDPUTKEEP
    0x12: ('putcat', TYPE_KV), # TTCMDPUTCAT
    0x13: ('putshl', 'putshl'), # TTCMDPUTSHL
    0x18: ('putnr', TYPE_KV), # TTCMDPUTNR
    0x20: ('out', TYPE_K), # TTCMDOUT
#    0x30: 'get', # TTCMDGET
#    0x31: 'mget', # TTCMDMGET
#    0x38: 'vsiz', # TTCMDVSIZ
#    0x50: 'iterinit', # TTCMDITERINIT
#    0x51: 'iternext', # TTCMDITERNEXT
#    0x58: 'fwmkeys', # TTCMDFWMKEYS
    0x60: ('addint', 'addint'), # TTCMDADDINT
    0x61: ('adddouble', 'adddouble'), # TTCMDADDDOUBLE
    0x68: ('ext', 'ext'), # TTCMDEXT
#    0x70: 'sync', # TTCMDSYNC
#    0x71: 'optimize', # TTCMDOPTIMIZE
    0x72: ('vanish', 'vanish'), # TTCMDVANISH
#    0x73: 'copy', # TTCMDCOPY
#    0x74: 'restore', # TTCMDRESTORE
#    0x78: 'setmst', # TTCMDSETMST
#    0x80: 'rnum', # TTCMDRNUM
#    0x81: 'size', # TTCMDSIZE
#    0x88: 'stat', # TTCMDSTAT
    0x90: ('misc', 'misc'), # TTCMDMISC
#    0xa0: 'repl' # TTCMDREPL
}


HEADER_FORMAT = struct.Struct('>BQHHI')


def UlogReader(name):
    '''
    Return a UlogDirReader if the path is a directory, and a UlogFileReader
    otherwise.
    '''
    if os.path.isdir(name):
        return UlogDirReader(name)
    else:
        return UlogFileReader(name)


class UlogFileReader(object):
    '''
    Read ulog records from a single file.
    '''
    def __init__(self, name):
        self.file = name
        self._f = open(name)
        self._body = None

    def iter(self):
        '''
        Iterate over the record headers in a file. Yields ts, sid, mid, size
        '''
        while True:
            header = self.read_header()
            if header is None:
                break
            yield header
            if self._body is None:
                self._f.seek(self.size, 1)
            self._body = None

    def read_header(self):
        self.header = self._f.read(HEADER_FORMAT.size)
        if len(self.header) == 0:
            return None
        (magic, self.ts, self.sid, self.mid, self.size) = HEADER_FORMAT.unpack(self.header)
        if magic != TCULMAGICNUM:
            print 'invalid magic'
            return None
        return self.ts, self.sid, self.mid, self.size

    def get_body(self):
        if self._body is None:
            self._body = self._f.read(self.size)
            self._offset = 2
        return self._body

    def get_command(self):
        return COMMANDS[ord(self.get_body()[1])][0]

    def get_command_type(self):
        return COMMANDS[ord(self.get_body()[1])][1]

    def _readstr(self, length=None):
        body = self.get_body()
        length = length or self._readlen()
        result = body[self._offset:self._offset + length]
        self._offset += length
        return result

    def _readlen(self):
        body = self.get_body()
        result = struct.unpack_from('>I', body, self._offset)[0]
        self._offset += 4
        return result

    def parse_body(self):
        command = self.get_command()
        command_type = self.get_command_type()
        body = self.get_body()
        exp = struct.unpack('>B', body[-1])[0]
        if command_type == TYPE_KV:
            ksiz = self._readlen()
            vsiz = self._readlen()
            args = (self._readstr(ksiz), self._readstr(vsiz))
        elif command_type == TYPE_K:
            args = (self._readstr(),)
        elif command_type == 'misc':
            offset = 2
            nsiz = self._readlen()
            anum = self._readlen()
            name = self._readstr(nsiz)
            misc_args = [self._readstr() for i in xrange(anum)]
            args = (name, misc_args)
        # TODO handle putshl, addint, addint, exp
        else:
            args = (body[self._offset:-1],)
        return command, command_type, args, exp

    def repl(self, callback):
        for ts, sid, mid, size in self.iter():
            command, command_type, args, exp = self.parse_body()
            if command == 'misc':
                name, misc_args = args
                if name == 'getlist':
                    continue
                elif name == 'putlist':
                    callback.misc_putlist(misc_args)
            else:
                getattr(callback, command)(*args)

    def close(self):
        self._f.close()


class UlogDirReader(object):
    def __init__(self, name):
        self._name = name
        self._files = filter(lambda f: f.endswith(".ulog"), sorted(os.listdir(name)))

    def iterfiles(self):
        previous_n = -1
        for f in self._files:
            n = int(f[:-5])
            if previous_n != -1 and n > (previous_n + 1):
                break
            previous_n = n
            reader = UlogFileReader('%s/%s' % (self._name, f))
            try:
                yield reader
            finally:
                reader.close()

    def iter(self):
        for reader in self.iterfiles():
            self._reader = reader
            for record in reader.iter():
                yield record

    def get_body(self):
        return self._reader.get_body()

    def get_command(self):
        return self._reader.get_command()

    def get_command_type(self):
        return self._reader.get_command_type()

    def parse_body(self):
        return self._reader.parse_body()

    def repl(self, callback):
        for reader in self.iterfiles():
            reader.repl(callback)

