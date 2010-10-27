import datetime
import itertools

import ulog


def hexify(string):
    return ' '.join('%02X' % ord(c) for c in string)


def to_pairs(l):
    a, b = itertools.tee(l)
    return itertools.izip(itertools.islice(a, 0, len(l), 2), itertools.islice(b, 1, len(l), 2))


def info(name):
    '''
    Print information about the first record in a ulog file.
    '''
    reader = ulog.UlogFileReader(name)
    ts, sid, mid, size = reader.read_header()
    print '%d\t%d:%d' % (ts, sid, mid)


def export(name):
    '''
    Print ulog records. Same as `ttulmgr export`.
    '''
    reader = ulog.UlogReader(name)
    for ts, sid, mid, size in reader.iter():
        body = reader.get_body()
        print '%d\t%d:%d\t%s\t%s' % (ts, sid, mid, reader.get_command(), hexify(body))


def pretty_export(name, max_length=None):
    '''
    Export readable arguments.
    '''
    max_length = max_length or 120
    reader = ulog.UlogReader(name)
    for ts, sid, mid, size in reader.iter():
        command, command_type, args, exp = reader.parse_body()
        t = datetime.datetime.fromtimestamp(ts / 1000000)
        arg_string = ', '.join(repr(arg) for arg in args)
        if len(arg_string) > max_length:
            arg_string = arg_string[:max_length - 3] + '...'
        print '%s\t%d:%d\t%s\t%s' % (t, sid, mid, command, arg_string)


def command_summary(name):
    '''
    Print the number of calls to each function.
    '''
    reader = ulog.UlogReader(name)
    summary = dict((command, (0, 0)) for command, command_type in ulog.COMMANDS.itervalues())
    try:
        for ts, sid, mid, size in reader.iter():
            command = reader.get_command()
            count, total_size = summary[command]
            count += 1
            total_size += size
            summary[command] = (count, total_size)
    except KeyboardInterrupt:
        print '(interrupted)'

    print '%12s%16s%16s' % ('FUNCTION', 'CALLS', 'SIZE')
    for command, (count, total_size) in sorted(summary.iteritems(), key=lambda (k, v): k):
        print '%12s%16d%16d' % (command, count, total_size)


def findrts(name, target_rts):
    '''
    Print the name of the ulog file that contains the given ts.
    '''
    dir_reader = ulog.UlogDirReader(name)
    previous_file = None
    for file_reader in dir_reader.iterfiles():
        ts, sid, mid, size = file_reader.read_header()
        if target_rts == ts:
            print 'in ' + file_reader.file
            return
        elif target_rts < ts:
            if previous_file is None:
                print 'before ' + file_reader.file
            else:
                print 'in ' + previous_file
            return
        previous_file = file_reader.file

    print 'after or in ' + file_reader.file


def ulogs_before(name, target_rts):
    '''
    Print the names of ulog files that occur before the given ts.

    Does not include the last ulog file, even if the ts does not occur in it.
    '''
    dir_reader = ulog.UlogDirReader(name)
    previous_file = None
    for file_reader in dir_reader.iterfiles():
        ts, sid, mid, size = file_reader.read_header()
        if target_rts <= ts:
            return
        if previous_file is not None:
            print previous_file
        previous_file = file_reader.file


def keyhistory(name, target_key):
    '''
    Print a summary of operations on the given key.
    '''
    reader = ulog.UlogReader(name)
    for ts, sid, mid, size in reader.iter():
        command, command_type, args, exp = reader.parse_body()
        if command_type == ulog.TYPE_KV and args[0] == target_key:
            print '%d\t%s\t%s' % (ts, command, repr(args[1]))
        elif command_type == ulog.TYPE_K and args[0] == target_key:
            print '%d\t%s' % (ts, command)
        elif command == 'misc' and args[0] == 'putlist':
            for key, value in to_pairs(args[1]):
                if key == target_key:
                    print '%d\tputlist\t%s' % (ts, repr(value))
        # TODO handle putshl, addint, addint, exp


if __name__ == '__main__':
    import sys
    command = sys.argv[1]

    if command == 'info':
        info(sys.argv[2])
    elif command == 'export':
        export(sys.argv[2])
    elif command == 'prettyexport':
        pretty_export(sys.argv[2])
    elif command == 'summary':
        command_summary(sys.argv[2])
    elif command == 'findrts':
        findrts(sys.argv[2], int(sys.argv[3]))
    elif command == 'ulogsbefore':
        ulogs_before(sys.argv[2], int(sys.argv[3]))
    elif command == 'keyhistory':
        keyhistory(sys.argv[2], sys.argv[3])
