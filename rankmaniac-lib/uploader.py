"""
Utility to execute map-reduce jobs on Amazon EMR.

Special notes:
    WARNING! Requires Python >= 2.5

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

from __future__ import with_statement # for Python 2.5

import sys, os
import ConfigParser
from time import sleep

from boto.exception import EmrResponseError
from rankmaniac import Rankmaniac

unbuff_stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # unbuffered

def do_main(team_id, access_key, secret_key,
            infile='input.txt', max_iter=50):
    """
    Submits a new map-reduce job to Amazon EMR and waits for it to
    finish executing.
    """

    # Ensure that the input file exists
    if not os.path.isfile(os.path.join('data', infile)):
        raise Exception('file %s not found' % (infile))

    # Default modules for where to expect the pagerank step
    # and process step code
    pagerank_map = 'pagerank_map.py'
    pagerank_reduce = 'pagerank_reduce.py'
    process_map = 'process_map.py'
    process_reduce = 'process_reduce.py'

    # Read the configuration and override defaults
    config = ConfigParser.SafeConfigParser()
    config.read('data/rankmaniac.cfg')

    section = 'Rankmaniac'
    if config.has_section(section):
        pagerank_map = config.get(section, 'pagerank_map')
        pagerank_reduce = config.get(section, 'pagerank_reduce')
        process_map = config.get(section, 'process_map')
        process_reduce = config.get(section, 'process_reduce')

    # Terminates the job and closes connections upon leaving this block
    with Rankmaniac(team_id, access_key, secret_key) as r:
        r.set_infile(infile)
        print('Uploading...')
        r.upload()
        print('Uploaded')

        print('Adding %d iterations...' % (max_iter))
        for i in range(max_iter):
            while True:
                try:
                    unbuff_stdout.write('.')
                    r.do_iter(pagerank_map, pagerank_reduce,
                              process_map, process_reduce)
                    break
                except EmrResponseError:
                    sleep(10) # call Amazon APIs infrequently
        print('')

        print('Waiting for map-reduce job to finish...')
        print('  Use Ctrl-C to interrupt')
        while True:
            try:
                unbuff_stdout.write('.')
                if r.is_done():
                    break
                elif not r.is_alive():
                    print('')
                    print("Failed to output 'FinalRank'!")
                    break
                sleep(20) # call Amazon APIs infrequently
            except EmrResponseError:
                sleep(60) # call Amazon APIs infrequently
            except KeyboardInterrupt:
                print('')
                break
        print('')

        print('Terminating...')
        print('  Downloading...')
        r.download()
        print('  Downloaded')

    print('Terminated')

if __name__ == '__main__':

    team_id = 'YOUR-TEAM-ID'
    access_key = 'YOUR-ACCESS-KEY'
    secret_key = 'YOUR-SECRET-KEY'

    do_main(team_id, access_key, secret_key)
