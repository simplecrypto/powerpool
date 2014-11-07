#!/usr/bin/python
import socket
import json
import argparse
import time
import logging


def wait_id(fo, target_id):
    for i in xrange(10):
        ret = json.loads(fo.readline())
        if ret['id'] == target_id:
            break
    else:
        raise Exception("No valid return in 10 reads!")
    return ret


def wait_method(fo, target_method):
    for i in xrange(10):
        ret = json.loads(fo.readline())
        if ret['method'] == target_method:
            break
    else:
        raise Exception("No valid return in 10 reads!")
    return ret


def main():
    start = int(time.time())

    parser = argparse.ArgumentParser(description='Check Stratum')
    parser.add_argument('-s', '--server', default='localhost',
                        help='the remote hostname')
    parser.add_argument('-p', '--port', type=int, default=3333,
                        help='the port to try and connect on')
    args = parser.parse_args()

    s = None
    for res in socket.getaddrinfo(args.server, args.port, socket.AF_UNSPEC,
                                  socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error:
            s = None
            continue

        try:
            s.connect(sa)
        except socket.error:
            s.close()
            s = None
            continue
        break

    if s is None:
        print 'could not open socket'
        return 2

    f = s.makefile()

    # subscribe
    f.write(json.dumps({u'params': [u'stratum_check/0.1'], u'id': 100,
                        u'method': u'mining.subscribe'}) + "\n")
    f.flush()
    t = time.time()
    ret = wait_id(f, 100)
    assert ret['error'] is None
    print 'stratum.{}.{}.subscribe {} {}'.format(
        args.server, args.port,
        (time.time() - t) * 1000,
        start)

    # authorize
    f.write(json.dumps({u'params': [u'testing', u''], u'id': 200, u'method':
                        u'mining.authorize'}) + "\n")
    f.flush()
    t = time.time()
    ret = wait_id(f, 200)
    assert ret['error'] is None
    print 'stratum.{}.{}.authorize {} {}'.format(
        args.server, args.port,
        (time.time() - t) * 1000,
        start)

    ret = wait_method(f, "mining.notify")
    print 'stratum.{}.{}.notify {} {}'.format(
        args.server, args.port,
        (time.time() - t) * 1000,
        start)

    # submit a job!
    t = time.time()
    f.write(json.dumps({u'params': [u'testing', ret['params'][0], u'00000000',
                                    u'545d2122', u'28030000'], u'method':
                        u'mining.submit', u'id': 300}) + "\n")
    f.flush()
    ret = wait_id(f, 300)
    assert ret['error'][0] is not None
    print 'stratum.{}.{}.share_process {} {}'.format(
        args.server, args.port,
        (time.time() - t) * 1000,
        start)

    f.close()
    s.close()


if __name__ == '__main__':
    try:
        exit(main())
    except Exception:
        logging.exception("Unhandled exception!")
        exit(2)
