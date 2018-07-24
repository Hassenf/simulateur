# =========================== adjust path =====================================

import os
import sys

if __name__ == '__main__':
    here = sys.path[0]
    sys.path.insert(0, os.path.join(here, '..'))

# ========================== imports ==========================================

import json
import glob
import matplotlib.pyplot as plt
import pandas as pd
import math

from SimEngine import SimLog

# =========================== defines =========================================

DAGROOT_ID = 0  # we assume first mote is DAGRoot
DAGROOT_IP = 0  # we assume DAGRoot IP is 0

# =========================== decorators ======================================

def openfile(func):
    def inner(inputfile):
        with open(inputfile, 'r') as f:
            return func(f)
    return inner

# =========================== helpers =========================================

@openfile
def drops_cdf(inputfile):

    allstats = {} # indexed by run_id, srcIp

    file_settings = json.loads(inputfile.readline())  # first line contains settings
    drops = {}
    asns = {}
    drop_count = {}

    # === gather raw stats

    for line in inputfile:
        logline = json.loads(line)

        # shorthands
        run_id = logline['_run_id']

        # populate
        if run_id not in drops:
            drops[run_id] = []
            asns[run_id] = []
            drop_count[run_id] = 0

        if logline['_type'] == SimLog.LOG_PACKET_DROPPED['type']:
            drop_count[run_id] += 1

            asn = logline['_asn']
            drops[run_id].append(drop_count[run_id])
            asns[run_id].append(asn)

    for run_id, data in drops.iteritems():
        plt.plot(asns[run_id], data)
    plt.show()

    return allstats

# =========================== main ============================================

def main():

    subfolders = list(
        map(
            lambda x: os.path.join('simData', x),
            os.listdir('simData')
        )
    )
    subfolder = max(subfolders, key=os.path.getmtime)
    for infile in glob.glob(os.path.join(subfolder, '*.dat')):
        print 'generating KPIs for {0}'.format(infile)

        # gather the kpis
        drops_cdf(infile)

if __name__ == '__main__':
    main()
