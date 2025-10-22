import random
import ray
import networkx as nx
from utils import register, Type
import sys
import hdfs
from storage import HDFS
import numpy as np
import igraph as ig

"""网络生成"""
# 随机图
@register(output_type=Type.GRAPH)
@ray.remote
def random_network(args, output_refs, locators):
    assert {'n', 'p', 'directed'} <= args.keys(), "Missing arguments for random_graph"
    n, p = int(args['n']), float(args['p'])
    directed = False if args['directed'] == 'False' else True
    return (nx.gnp_random_graph(n, p, directed=directed),)

@register(output_type=Type.GRAPH)
@ray.remote
def star_network(args, output_refs, locators):
    assert {'n'} <= args.keys(), "Missing arguments for star_graph"
    n = int(args['n'])
    return (nx.star_graph(n),)


@register(output_type=Type.GRAPH)
@ray.remote
def small_world_network(args, output_refs, locators):
    assert {'n', 'k', 'p'} <= args.keys(), "Missing arguments for small_world_network"
    n, k, p = int(args['n']), int(args['k']), float(args['p'])
    return (nx.watts_strogatz_graph(n, k, p),)

# 无标度网络
@register(output_type=Type.GRAPH)
@ray.remote
def scale_free_network(args, output_refs, locators):
    assert 'n' in args and 'm' in args, "Missing arguments for scale_free_network"
    n, m = int(args['n']), int(args['m'])
    return (nx.barabasi_albert_graph(n, m),)

    # """
    # 生成一个最近邻耦合网络。
    # 参数:
    #     n (int): 节点数量。
    #     k (int): 每个节点的最近邻数。
    #     p (float): 添加额外边的概率。
    # 返回:
    #     networkx.Graph: 最近邻耦合网络。
    # """
# @register(output_type=Type.GRAPH)
# @ray.remote
# def nearest_neighbor_coupling_network(args, output_refs, locators):
#     assert {'n', 'p', 'k'} <= args.keys(), "Missing arguments for nearest_neighbor_coupling_network"
#     n, k, p = int(args['n']), int(args['k']), float(args['p'])
#
#     G = nx.Graph()
#     nodes = list(range(n))
#     G.add_nodes_from(nodes)
#     for i in range(n):
#         for j in range(1, k+1):
#             neighbor = (i + j) % n  # 计算最近邻节点
#             G.add_edge(i, neighbor)
#
#     # 随机添加额外边
#     for i in range(n):
#         for j in range(i+1, n):
#             if np.random.rand() < p:
#                 G.add_edge(i, j)
#     return (G,)

"""
全局耦合网络
"""
@register(output_type=Type.GRAPH)
@ray.remote
def complete_graph(args, output_refs, locators):
    assert {'n'} <= args.keys(), "Missing arguments for complete_graph"
    n = int(args['n'])
    return (nx.complete_graph(n),)


"""
网络加载
"""
# 加载Cora数据集
@register(output_type=Type.GRAPH)
@ray.remote
def loadCoraFromFile(args, output_refs, locators):
    cora_content, cora_cites = HDFS().load_cora_from_hdfs()
    # 根据cora生成网络
    G = nx.Graph()
    G.add_nodes_from(np.array(cora_content[0]))
    G.add_edges_from(np.array(cora_cites))
    return (G,)

# 算子的参数格式: 文件路径、有向图无向图、加载数据文件格式
@register(output_type=Type.GRAPH)
@ray.remote
def load_graph_from_csv(args, output_refs, locators):
    assert {'edgesFilePath'} <= args.keys(), "Missing arguments for load_graph_from_csv"
    # 加载边文件构建网络
    edge_content = HDFS().load_edges_from_csv(args['edgesFilePath'])
    G = nx.Graph()
    id_mapping = {}  # 用于将原始ID映射到新ID

    for line in edge_content.strip().split('\n'):
        source, target, weight = line.strip().split(',')
        if source not in id_mapping:
            new_id_source = len(id_mapping)
            id_mapping[source] = new_id_source
        if target not in id_mapping:
            new_id_target = len(id_mapping)
            id_mapping[target] = new_id_target

        G.add_edge(id_mapping[source], id_mapping[target], weight=float(weight))

    nodeAttributesFilePath = args.get('nodeAttributesFilePath')
    if nodeAttributesFilePath != "":
        # 将节点属性关联到图中的节点
        node_attributes = HDFS().load_node_attributes_from_csv(nodeAttributesFilePath)
        for node_id, attributes in node_attributes.items():
            if node_id in id_mapping:
                G.nodes[id_mapping[node_id]]['id'] = id_mapping[node_id]  # 更新节点ID属性
                G.nodes[id_mapping[node_id]]['name'] = node_id  # 将节点id作为name
                G.nodes[id_mapping[node_id]]['attributes'] = attributes

    return (G,)


#
# # 快速随机图（p很小时生成更快），适用于稀疏图
# @register(output_type=Type.GRAPH)
# @ray.remote
# def gnp_random_sparse_graph(args, output_refs, locators):
#     assert {'n', 'p', 'directed'} <= args.keys(), "Missing arguments for RandomGraphGeneration"
#     n, p, directed = args['n'], args['p'], args['directed']
#     return (nx.fast_gnp_random_graph(n, p, directed=directed),)
#
#
# # 指定结点数和边数的随机图
# @register(output_type=Type.GRAPH)
# @ray.remote
# def gnm_random_graph(args, output_refs, locators):
#     assert {'n', 'm', 'directed'} <= args.keys(), "Missing arguments for RandomGraphGeneration"
#     n, m, directed = args['n'], args['m'], args['directed']
#     return (nx.gnm_random_graph(n, m, directed=directed),)
#
#
# # 快速随机图，适用于稠密图
# @register(output_type=Type.GRAPH)
# @ray.remote
# def gnm_random_dense_graph(args, output_refs, locators):
#     assert 'n' in args and 'm' in args, "Missing arguments for RandomGraphGeneration"
#     n, m = args['n'], args['m']
#     return (nx.dense_gnm_random_graph(n, m),)
#
#
# # 星型网络
# @register(output_type=Type.GRAPH)
# @ray.remote
# def star_graph(args, output_refs, locators):
#     assert 'n' in args, "Missing arguments for StarGraphGeneration"
#     n = args['n']
#     return (nx.star_graph(n),)


# 最近邻耦合网络
@register(output_type=Type.GRAPH)
@ray.remote
def nearest_neighbor_coupling_network(args, output_refs, locators):
     assert 'n' in args and 'k' in args, "Missing arguments for nearest_neighbor_coupling_network"
     n, k = int(args['n']), int(args['k'])
     half_k = k // 2
     NNC_net = nx.Graph()
     NNC_net.add_nodes_from(range(n))
     NNC_net.add_edges_from([(i, j % n) for i in range(n) for j in range(i - half_k, i + half_k + 1) if j != i])
     return (NNC_net,)


# # 随机权重网络 np
# @register(output_type=Type.GRAPH)
# @ray.remote
# def gnp_random_weighted_graph(args, output_refs, locators):
#     assert {'n', 'p', 'directed'} <= args.keys(), "Missing arguments for gnp_random_weighted_graph"
#     n, p, directed = args['n'], args['p'], args['directed']
#     if {'a', 'b'} <= args.keys():
#         a, b = args['a'], args['b']
#     else:
#         a, b = 0, 1
#     G = nx.gnp_random_graph(n, p, directed=directed)
#     for (u, v, d) in G.edges(data=True):
#         d['weight'] = random.uniform(a, b)
#     return (G,)
#
#
# # 随机权重网络 nm
# @register(output_type=Type.GRAPH)
# @ray.remote
# def gnm_random_weighted_graph(args, output_refs, locators):
#     assert {'n', 'm', 'directed'} <= args.keys(), "Missing arguments for gnm_random_weighted_graph"
#     n, m, directed = args['n'], args['m'], args['directed']
#     if {'a', 'b'} <= args.keys():
#         a, b = args['a'], args['b']
#     else:
#         a, b = 0, 1
#     G = nx.gnm_random_graph(n, m, directed=directed)
#     for (u, v, d) in G.edges(data=True):
#         d['weight'] = random.uniform(a, b)
#     return (G,)

