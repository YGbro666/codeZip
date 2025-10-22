import json
from typing import Dict, List, Set, Tuple, Union, Generator, Iterator
import networkx as nx
from enum import Enum
import ray
import numpy as np
from collections import defaultdict


class Type(Enum):
    GRAPH = Union[nx.Graph, nx.DiGraph]
    UN_GRAPH = nx.Graph
    DI_GRAPH = nx.DiGraph
    BOOL = bool
    INT = int
    FLOAT = float
    DICT = Dict
    CLUSTER = List
    LIST = List
    SET = Set
    TUPLE = Tuple
    GENERATOR = Generator
    ITERATOR = Iterator


def isDisplay(type):
    #此处保存的为可视化组件，不参与运算
    displayType=['rayGraphShow', 'graphShow', 'rayGraphResultShow']
    if(displayType.count(type)==0):
        return False
    return True


def make_list(obj):
    if isinstance(obj, list):
        return obj
    elif obj is not None:
        return [obj]
    else:
        return []


def register(input_type=None, output_type=None):
    def foo(f):
        f.input_type = make_list(input_type)
        f.output_type = make_list(output_type)
        return f

    return foo


def nxgraph2json(graph: Type.GRAPH, name):
    nodes, edges = graph.nodes, graph.edges
    is_directed = graph.is_directed()  # 判断图是否是有向图
    graph_data = {
        "edge": {
            "data": [],
            "directed": is_directed
        },
        "vertex": {
            "data": []
        },
        "cluster": []
    }
    for edge in edges:
        source = str(edge[0])
        target = str(edge[1])
        weight = 1 if 'weight' not in graph.edges[edge] else graph.edges[edge]['weight']
        graph_data["edge"]["data"].append({
            "source": source,
            "target": target,
            "weight": weight
        })

    # 创建一个字典，默认值为列表
    cluster_dict = defaultdict(list)

    for node in nodes:
        node_id = node
        node_name = str(node_id) if 'name' not in graph.nodes[node] else graph.nodes[node]['name']
        cluster = -1 if 'cluster' not in graph.nodes[node] else graph.nodes[node]['cluster']  # 默认群集为 -1
        # 将节点添加到对应的类别列表中
        if cluster != -1:
            cluster_dict[cluster].append(node_id)
        # class_value = -1 if 'class' not in graph.nodes[node] else graph.nodes[node]['class']  # 默认类别为 -1
        weight = 1 if 'weight' not in graph.nodes[node] else graph.nodes[node]['weight']

        # 检查节点是否包含 attributes 属性
        if 'attributes' in graph.nodes[node]:
            attributes = graph.nodes[node]['attributes']
            if isinstance(attributes, set):
                attributes = list(attributes)  # 将集合转换为列表
        else:
            attributes = {}

        graph_data["vertex"]["data"].append({
            "id": node_id,
            "name": node_name,
            **({'cluster': cluster} if cluster != -1 else {}),
            **({'class': graph.nodes[node]['class']} if 'class' in graph.nodes[node] else {}),
            "weight": weight,
            **{'attributes': attributes}  # 添加 attributes 到 JSON 数据中
        })

    # 将cluster_dict转换为二维数组或列表
    cluster_list = list(cluster_dict.values())
    graph_data['cluster'] = cluster_list
    return json.dumps(graph_data, ensure_ascii=False)


def list2json(lists: Type.LIST, name):
    tmp = []
    for item in lists:
        tmp.append(item)
    data = {
        "List_number": len(lists),
        "lists": tmp
    }
    return json.dumps(data, ensure_ascii=False, default=default_dump)

def dict2json(dicts:Type.DICT, name):
    return json.dumps(dicts, default=default_dump)

def tuple2json(tuples:Type.TUPLE, name):
    data = ""
    if name == "louvain_communities":
        # for item in tuples:
        #     if type(item) in special_types:
        #         data += special_types[type(item)](item, name)
        data += special_types[type(tuples[0])](tuples[0], name)[:-1]
        data += ', \"cluster\": [' + ', '.join([str(sublist) for sublist in tuples[1]]) + ']}'
    return data

def default_dump(obj):
    """Convert numpy classes to JSON serializable objects."""
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


special_types = {
    nx.Graph: nxgraph2json,
    nx.DiGraph: nxgraph2json,
    list: list2json,
    dict: dict2json,
    tuple: tuple2json
}


def adjust_the_output_to_json(output, name):
    if type(output) in special_types:
        return special_types[type(output)](output, name)
    return json.dumps(output, ensure_ascii=False)


def fetch(output_refs, locator):
    return ray.get(output_refs[locator[0]])[locator[1]]

# 可视化组件并不需要参与运算
def rearrange_job_info_format(job_info):
    new_job_info = {}
    for node in job_info['nodes']:
        if node['group'] == 'graphVisualization':
            continue
        t = new_job_info[node['mark']] = {}
        t['name'] = node['name']
        t['args'] = {}
        for attribute in node['simpleAttributes']:
            t['args'][attribute['name']] = attribute['value']
        t['locators'] = {}
        for anchor in node['inputAnchors']:
            t['locators'][anchor['seq']] = (anchor['sourceAnchor']['nodeMark'], anchor['sourceAnchor']['seq'])
    return new_job_info

import pandas as pd
def cora_pretreatment(cora_content, cora_cites):
    content_idx = list(cora_content.index)  # 将索引制作成列表
    paper_id = list(cora_content.iloc[:, 0])  # 将content第一列取出
    mp = dict(zip(paper_id, content_idx))  # 映射成{论文id:索引编号}的字典形式

    # 切片提取从第一列到倒数第二列（左闭右开）
    feature = cora_content.iloc[:, 1:-1]
    label = cora_content.iloc[:, -1]  # 提取最后一列
    label = pd.get_dummies(label)  # 独热编码

    mat_size = cora_content.shape[0]  # 第一维的大小2708就是邻接矩阵的规模
    adj_mat = np.zeros((mat_size, mat_size))  # 创建0矩阵

    # 创建邻接矩阵
    for i, j in zip(cora_cites[0], cora_cites[1]):  # 枚举形式（u，v）
        x = mp[i]
        y = mp[j]
        adj_mat[x][y] = 1

    return feature, label, adj_mat




