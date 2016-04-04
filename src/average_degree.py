#!/usr/bin/env python
# example of program that calculates the average degree of hashtags in a twitter graph

__author__ = "Wenbo Ren: rainbow.ren@mail.utoronto.ca"

import json
import sys
import datetime
import heapq
from collections import namedtuple
from collections import defaultdict


# find the edges to evict, exclude the ones that exist in the new graph
def find_orphan_set(hashtag, old_set, slide_window):
    non_orphan_set = set([])
    # for each edge (to be evicted) in the graph, check whether there exists same edge in the new graph
    # if yes, keep the edge; if no, ready to evict
    for h in old_set:
        for m in slide_window:
            # the edge exist in the current slide_window
            if set([hashtag, h]).issubset(m.hashtags):
                non_orphan_set.update([h])
                break
    return old_set.difference(non_orphan_set)


# evict the expired edges
def prune_graph(g, evict_list, slide_window):
    for m in evict_list:
        for t in m.hashtags:
            # some edges in the evict_list may still exist in the new graph,
            # exclude these edges and find the set of nodes (or edges) to evict
            update_set = find_orphan_set(t, m.hashtags.difference([t]), slide_window)
            # update the graph, remove unconnected nodes
            g[t].difference_update(update_set)
            if len(g[t]) == 0:
                g.pop(t, None)


# add the new edges to the graph
def add_to_graph(g, hashtags):
    if len(hashtags) < 2:
        return
    for h in hashtags:
        g[h].update(hashtags.difference([h]))


# go through the graph and calculate the average degree
def calculate_average_degree(g):
    if not g:
        return "0.00"
    sum_degrees = 0
    for k in g:
        sum_degrees += len(g[k])
    return "{0:.3f}".format(float(sum_degrees)/len(g))[:-1]


def run(input_file, output_file):
    Message = namedtuple('Message', 'timestamp hashtags')
    # the graph is constructed as a dict of sets. keys - nodes, values - set of nodes connected to the key node
    graph = defaultdict(set)
    max_timestamp = datetime.datetime.min
    time_delta = datetime.timedelta(seconds=60)
    slide_window = []
    result = "0.00"
    try:
        f = open(input_file, 'r')
        output_file_handler = open(output_file, 'w')
    except IOError:
        sys.stderr.write("Error opening the input or output file")
        exit(1)

    with f:
        for line in f:
            try:
                msg = json.loads(line.strip())
            except ValueError:
                sys.stderr.write("Invalid Message")
                continue
            # remove traffic messages
            if "limit" in msg and "track" in msg["limit"]:
                continue
            # ignore messages without timestamp
            if "created_at" not in msg:
                continue
            ts = datetime.datetime.strptime(msg["created_at"], "%a %b %d %H:%M:%S +0000 %Y")
            hashtags = set([])
            if "entities" in msg and "hashtags" in msg["entities"]:
                hashtags = set([m["text"] for m in msg["entities"]["hashtags"]])

            max_timestamp = max(ts, max_timestamp)
            min_timestamp = max_timestamp - time_delta
            # ignore expired messages and messages with single/no hashtag
            if ts < min_timestamp:
                output_file_handler.write(result+'\n')
                continue
            # push the message into the slide_window - a heap
            msg = Message(timestamp=ts, hashtags=hashtags)
            heapq.heappush(slide_window, msg)
            if graph and slide_window:
                # nodes to evict from te graph
                evict_list = []
                while slide_window[0].timestamp < min_timestamp:
                    evict_list.append(heapq.heappop(slide_window))
                prune_graph(graph, evict_list, slide_window)
            if slide_window:
                add_to_graph(graph, hashtags)
            # calculate the average degree based on the graph
            result = calculate_average_degree(graph)
            output_file_handler.write(result+'\n')
    output_file_handler.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Not enough arguments, usage: python average_degree.py input_file_path output_file_path")
        exit(1)
    run(sys.argv[1], sys.argv[2])
