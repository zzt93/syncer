import argparse
import subprocess


def parseArgs():
    global args
    parser = argparse.ArgumentParser(description='A binlog backup utility')
    parser.add_argument('-H', '--host', dest='host', action='store',
                        default='localhost',
                        help='mysql server host (default: localhost)')
    parser.add_argument('-d', '--dir', dest='dir', action='store',
                        default='./',
                        help='binlog store path (default: ./)')
    parser.add_argument('-P', '--port', dest='port', action='store',
                        default=3306,
                        help='mysql server port (default: 3306)')
    parser.add_argument('-u', '--user', dest='user', action='store',
                        default='root',
                        help='mysql server user name (default: root)')
    parser.add_argument('-p', '--password', dest='password', action='store', required=True,
                        help='mysql server user password (no default)')
    args = parser.parse_args()


if __name__ == '__main__':
    parseArgs()
    print args
    result = subprocess.check_output(['mysql', '-u', args.user, '-p'+args.password, '-P', args.port, '-h', args.host, '-e', 'SHOW BINARY LOGS;'])
    print(result)
    dump = ['mysqlbinlog', '-u', args.user, '-p' + args.password, '-P', args.port, '-h', args.host, '--raw',
            '--result-file', args.dir, '-R']
    for line in result.split('\n')[1:]:
        dump.append(line.split('\t')[0])
    result = subprocess.check_output(dump)

