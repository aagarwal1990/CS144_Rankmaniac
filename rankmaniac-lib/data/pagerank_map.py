#!/usr/bin/env python

import sys
import re
#
# This program simply represents the identity function.
#

adj_dict = {}
nodes = []

def stringToList(node_str):
    return re.split('NodeId:|,|\t|\n', node_str)
    
for line in sys.stdin:
    line = stringToList(line)
    nodeID = int(line[1])
    curr_rank = float(line[2])
    prev_rank = float(line[3])
    neighbours = [int(x) for x in line[4:-2]]
    adj_dict[nodeID] = {'prev_rank': prev_rank, 'curr_rank': curr_rank, \
                        'neighbours': neighbours}
    nodes.append(nodeID)
    
for i in range(len(nodes)):
    curr_node = nodes[i]
    adj_dict[curr_node]['prev_rank']= adj_dict[curr_node]['curr_rank'] 
    adj_dict[curr_node]['curr_rank'] = float(i)
    sys.stdout.write(str(curr_node) + ':::' + str(adj_dict[curr_node]['prev_rank']) \
                     + ':::' + str(adj_dict[curr_node]['curr_rank']) \
                     + ':::' + str(adj_dict[curr_node]['neighbours']) +'\n')
    

    

    

