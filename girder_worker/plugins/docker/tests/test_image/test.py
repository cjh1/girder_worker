import sys
import time
import signal

f = open('/mnt/girder_worker/data/log', 'w')
f.write('here\n')
f.flush()

if len(sys.argv) == 3:
    mode = sys.argv[1]
    message = sys.argv[2]

    if mode == 'stdio':
        print(message)
    elif mode == 'output_pipe':
        with open('/mnt/girder_worker/data/output_pipe', 'w') as fp:
            fp.write(message)
    elif mode == 'input_pipe':
        f.write('before\n')
        f.flush()
        with open('/mnt/girder_worker/data/input_pipe', 'r') as fp:
            f.write('reading\n')
            f.flush()
            print(fp.read())
            f.write('read\n')
            f.flush()
    elif mode == 'sigkill':
        time.sleep(30)
    elif mode == 'sigterm':
        def _signal_handler(signal, frame):
            sys.exit(0)
        # Install signal handler
        signal.signal(signal.SIGTERM, _signal_handler)
        time.sleep(30)
    elif mode == 'stdout_stderr':
        sys.stdout.write('this is stdout data\n')
        sys.stderr.write('this is stderr data\n')
    else:
        sys.stderr.write('Invalid test mode: "%s".\n' % mode)
        sys.exit(-1)
else:
    sys.stderr.write('Insufficient arguments.\n')
    sys.exit(-1)
