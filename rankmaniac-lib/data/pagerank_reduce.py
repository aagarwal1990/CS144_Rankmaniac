#!/usr/bin/env python

import sys
import re
#
# This program simply represents the identity function.
#

def stringToList(node_str):
    return re.split("\s|\n|'['|']'", node_str)

results_dict = {}   
prevNode = None 
prevRank = None

for line in sys.stdin:
    line = stringToList(line)
    currNode = int(line[0])
    currRank = float(line[2])
    if currNode == prevNode:
        # perform reduce operation on node
        prevRank += currRank
    else:
        # add previous node to our results dict
        if prevNode is not None:
            results_dict[prevNode] = prevRank
        # restart reduce process with new node
        prevNode = currNode
        prevRank = currRank
        
    print str(line) + "\n"

