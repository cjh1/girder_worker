import sys

with open(sys.argv[1], 'r') as fd:
    for line in fd:
        with open('/tmp/input.txt', 'a') as fp:
            print >> fp, '|%s|' % line.strip()[::-1]
            print(line.strip()[::-1])
