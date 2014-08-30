#!/usr/bin/env python
import argparse
import os
import subprocess

parser = argparse.ArgumentParser(prog='simplecoin RPC')
parser.add_argument('-l', '--log-level',
                    choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
subparsers = parser.add_subparsers(title='main subcommands', dest='action')

subparsers.add_parser('create')
link = subparsers.add_parser('link', help='links a list of executable names to the current git revision')
link.add_argument('names', help='names of the hardlinks you\'d like to create', action='append')

args = parser.parse_args()
githash = subprocess.check_output("git rev-parse HEAD", shell=True).strip()
print("{} hash identified as current!".format(githash))
assert len(githash) == 40
basedir = "../{}".format(githash)


def req(call):
    ret = system(call)
    if ret:
        print "\n#### ERROR ####\n"
        exit(ret)


def system(call):
    print "-- {}".format(call)
    return os.system(call)


def try_pip(pip_fragment):
    pip_inst = ("{}/bin/pip install --no-index --use-wheel --find-links='../wheelhouse/'"
                " --download-cache='../pipcache' {}".format(basedir, pip_fragment))
    # If error (non zero ret)
    if system(pip_inst):
        req("{}/bin/pip wheel --download-cache='../pipcache' "
            " --wheel-dir='../wheelhouse' {}".format(basedir, pip_fragment))
    req(pip_inst)

if args.action == "create":
    req("virtualenv {}".format(basedir))
    req("{}/bin/pip install wheel".format(basedir))
    try_pip("-r requirements.txt")
    try_pip("vtc_scrypt ltc_scrypt drk_hash")
    req("{}/bin/pip install -e .".format(basedir))
    print "\n#### SUCCESS ####\n"

elif args.action == "link":
    for name in args.names:
        req("ln -f {}/bin/pp {}".format(basedir, name))
