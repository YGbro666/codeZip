from graph_processing import *
from graph_generation import *
from utils import isDisplay
import json


def topological_sort(job_info):
    # Extract information from
    nodes, adj_lt, indegree = [], dict(), dict()
    for node in job_info['nodes']:
        if isDisplay(node['name']):
            continue
        nodes.append(node['mark'])
        t = adj_lt[node['mark']] = []
        for anchor in node['outputAnchors']:
            for target_anchor in anchor['targetAnchors']:
                if not isDisplay(target_anchor['nodeName']):
                    t.append(target_anchor['nodeMark'])
        indegree[node['mark']] = len(node['inputAnchors'])

    # For a point with no input, we consider it the starting point of a separate branch
    zero_in_nodes = []
    for node in nodes:
        if indegree[node] == 0:
            zero_in_nodes.append(node)

    toposorted = []
    while len(toposorted) < len(nodes):
        if len(zero_in_nodes) == 0:
            raise ValueError("Input graph contains loop")

        new_zero_in_nodes = []
        for node in zero_in_nodes:
            for nxt in adj_lt[node]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    new_zero_in_nodes.append(nxt)

        toposorted.extend(zero_in_nodes)
        zero_in_nodes = new_zero_in_nodes

    return toposorted


def validate_type(job_info):
    for node in job_info['nodes']:
        if isDisplay(node['name']):
            continue
        func = eval(node["name"])
        if len(node["inputAnchors"]) != len(func.input_type):
            raise TypeError("Flow type miss match")
        for anchor in node['inputAnchors']:
            seq = anchor['seq']
            expected_type = func.input_type[seq]
            src_type, src_seq = anchor['sourceAnchor']['nodeName'], anchor['sourceAnchor']['seq']
            read_type = eval(src_type).output_type[src_seq]
            if expected_type != read_type:
                raise TypeError("Flow type miss match")