#! /usr/bin/env python
import Pyro4
import daemon
import sys


def main():
   Pyro4.naming.startNSloop()

if sys.argv[1] == "-d":
  with daemon.DaemonContext():
    main()
else:
  main()
