#!/usr/bin/env python

import sys
import re
#
# This program simply represents the identity function.
#

def stringToList(node_str):
    return re.split(":::|\n|'", node_str)

results_dict = {}   
prevNode = None 
prevOldRank = None
prevNewRank = None
prevNeighbours = None

for line in sys.stdin:
    line = stringToList(line)
    
    currNode = int(line[0])
    currOldRank = float(line[1])
    currNewRank = float(line[2])
    currNeighbours =line[3]
    
    if currNode == prevNode:
        # perform reduce operation on node
        prevNewRank += currNewRank
    else:
        # add previous node to our results dict
        if prevNode is not None:
            results_dict[prevNode] = (prevOldRank, prevNewRank, prevNeighbours)
        # restart reduce process with new node
        prevNode = currNode
        prevOldRank = currOldRank
        prevNewRank = currNewRank
        prevNeighbours = currNeighbours

for node, (oldRank, newRank, neighbours) in results_dict.iteritems():
    print node, oldRank, newRank, neighbours
    sys.stdout.write(str(node) + ':::' + str(oldRank) + ':::' + \
                     ':::' + str(newRank) + ':::' + str(neighbours) + '\n')    


