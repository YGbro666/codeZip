from typing import Union, Generator
import ray
import networkx as nx
import networkx.algorithms.community as nx_comm
from utils import register, Type, fetch
import igraph as ig
import community
from sklearn.cluster import SpectralClustering
import numpy as np

'''节点的排序特征'''


# 度中心性、入度中心性和出度中心性
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def degree_centrality(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    result = dict()
    result["degree_centrality"] = nx.degree_centrality(G)
    if isinstance(G, nx.DiGraph):
        result["in_degree_centrality"] = nx.in_degree_centrality(G)
        result["out_degree_centrality"] = nx.out_degree_centrality(G)
    return (result,)


# 中介中心性/中间中心性/介数中心性
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def betweenness_centrality(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    return (nx.betweenness_centrality(G),)


# 接近中心性
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def closeness_centrality(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    return (nx.closeness_centrality(G),)


# 特征向量中心性
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def eigenvector_centrality(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    return (nx.eigenvector_centrality(G),)


# PageRank计算
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def pagerank_centrality(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    return (nx.pagerank(G),)

# 网络稀疏性
# 概率密度
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def network_sparsity(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    # 计算网络的稠密程度（稀疏性的补数）
    density = nx.density(G)

    # 返回结果
    result_dict = {"density": density}
    return (result_dict,)

"""节点与边的统计"""

@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def degree_histogram(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    result_dict = {"degree_histogram": nx.degree_histogram(G)}
    return (result_dict,)


'''生成树'''


# generateMST

# 最小生成树
@register(input_type=Type.GRAPH, output_type=Type.UN_GRAPH)
@ray.remote
def minimum_spanning_tree(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    weight = args['weight'] if 'weight' in args else 'weight'
    algorithm = args['algorithm'] if 'algorithm' in args else 'kruskal'
    ignore_nan = args['ignore_nan'] if 'ignore_nan' in args else False
    return (nx.minimum_spanning_tree(G, weight, algorithm, ignore_nan),)


#
# # 最大生成树
# @register(input_type=Type.GRAPH, output_type=Type.UN_GRAPH)
# @ray.remote
# def maximum_spanning_tree(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     weight = args['weight'] if 'weight' in args else 'weight'
#     algorithm = args['algorithm'] if 'algorithm' in args else 'kruskal'
#     ignore_nan = args['ignore_nan'] if 'ignore_nan' in args else False
#     return (nx.maximum_spanning_tree(G, weight, algorithm, ignore_nan),)
#
#
# # 随机生成树
# @register(input_type=Type.GRAPH, output_type=Type.UN_GRAPH)
# @ray.remote
# def random_spanning_tree(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     weight = args['weight'] if 'weight' in args else None
#     multiplicative = args['multiplicative'] if 'multiplicative' in args else True
#     seed = args['seed'] if 'seed' in args else None
#     return (nx.random_spanning_tree(G, weight, multiplicative, seed),)
#
#
# '''无向图连通性'''
#
#
# # 判断连通性
# @register(input_type=Type.UN_GRAPH, output_type=Type.BOOL)
# @ray.remote
# def is_connected(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.is_connected(G),)
#
#
# # 连通分量数
# @register(input_type=Type.UN_GRAPH, output_type=Type.INT)
# @ray.remote
# def number_connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.number_connected_components(G),)
#
#
# # 连通分量
# @register(input_type=Type.UN_GRAPH, output_type=Type.LIST)
# @ray.remote
# def connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return ([c for c in nx.connected_components(G)],)
#
#
# # 特定结点所在的连通分量
# @register(input_type=Type.UN_GRAPH, output_type=Type.SET)
# @ray.remote
# def node_connected_component(args, output_refs, locators):
#     assert 'n' in args, "Missing arguments for NodeConnectedComponent"
#     n = args['n']
#     G = fetch(output_refs, locators[0])
#     return (nx.node_connected_component(G, n),)
#
#
# '''有向图连通性'''
#
#
# # 判断强连通性
# @register(input_type=Type.DI_GRAPH, output_type=Type.BOOL)
# @ray.remote
# def is_strongly_connected(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.is_strongly_connected(G),)
#
#
# # 强连通分量数
# @register(input_type=Type.DI_GRAPH, output_type=Type.INT)
# @ray.remote
# def number_strongly_connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.number_strongly_connected_components(G),)
#
#
# # 强连通分量
# @register(input_type=Type.DI_GRAPH, output_type=Type.LIST)
# @ray.remote
# def strongly_connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return ([c for c in nx.strongly_connected_components(G)],)
#
#
# # 判断弱连通性
# @register(input_type=Type.DI_GRAPH, output_type=Type.BOOL)
# @ray.remote
# def is_weakly_connected(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.is_weakly_connected(G),)
#
#
# # 弱连通分量数
# @register(input_type=Type.DI_GRAPH, output_type=Type.INT)
# @ray.remote
# def number_weakly_connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return (nx.number_weakly_connected_components(G),)
#
#
# # 弱连通分量
# @register(input_type=Type.DI_GRAPH, output_type=Type.LIST)
# @ray.remote
# def weakly_connected_components(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     return ([c for c in nx.weakly_connected_components(G)],)
#
#
# '''最短路径'''
#
#
# # 最短路径（一条）
# # @register(input_type=Type.GRAPH, output_type=Union[Type.LIST, Type.DICT])
# @ray.remote
# def shortest_path(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     source = args['source'] if 'source' in args else None
#     target = args['target'] if 'target' in args else None
#     weight = args['weight'] if 'weight' in args else None
#     method = args['method'] if 'method' in args else 'dijkstra'
#     return (nx.shortest_path(G, source, target, weight, method),)
#
#
# # 特定结点对间的所有最短路径
# @register(input_type=Type.GRAPH, output_type=Type.LIST)
# @ray.remote
# def all_shortest_paths(args, output_refs, locators):
#     assert {'source', 'target'} <= args.keys(), "Missing arguments for AllShortestPaths"
#     G = fetch(output_refs, locators[0])
#     source, target = args['source'], args['target']
#     weight = args['weight'] if 'weight' in args else None
#     method = args['method'] if 'method' in args else 'dijkstra'
#     return ([p for p in nx.all_shortest_paths(G, source, target, weight, method)],)
#
#
# # 最短路径长度
# # @register(input_type=Type.GRAPH, output_type=Union[Type.INT, Type.DICT])
# @ray.remote
# def shortest_path_length(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     source = args['source'] if 'source' in args else None
#     target = args['target'] if 'target' in args else None
#     weight = args['weight'] if 'weight' in args else None
#     method = args['method'] if 'method' in args else 'dijkstra'
#     p = nx.shortest_path_length(G, source, target, weight, method)
#     return (dict(p) if isinstance(p, Generator) else p,)
#
#
# # 平均最短路径长度
# # @register(input_type=Type.GRAPH, output_type=Union[Type.INT, Type.FLOAT])
# @ray.remote
# def average_shortest_path_length(args, output_refs, locators):
#     G = fetch(output_refs, locators[0])
#     weight = args['weight'] if 'weight' in args else None
#     method = 'unweighted' if weight is None else args['method'] if 'method' in args else 'dijkstra'
#     return (nx.average_shortest_path_length(G, weight, method),)
#
#
# # 给定结点对间是否存在路径
# @register(input_type=Type.GRAPH, output_type=Type.BOOL)
# @ray.remote
# def has_path(args, output_refs, locators):
#     assert {'source', 'target'} <= args.keys(), "Missing arguments for HasPath"
#     G = fetch(output_refs, locators[0])
#     source, target = args['source'], args['target']
#     return (nx.has_path(G, source, target),)
#
#
# '''社团'''
#
#
#
#
# # 模块性
# @register(input_type=Type.GRAPH, output_type=Type.FLOAT)
# @ray.remote
# def modularity(args, output_refs, locators):
#     assert 'community' in args, "Missing arguments for Modularity"
#     G = fetch(output_refs, locators[0])
#     community = args['community']
#     weight = args['weight'] if 'weight' in args else "weight"
#     resolution = args['resolution'] if 'resolution' in args else 1
#     return (nx_comm.modularity(G, community, weight, resolution),)
#
#

# Lovain社团发现算法
@register(input_type=Type.GRAPH, output_type=Type.GRAPH)
@ray.remote
def louvain_communities(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    weight = args['weight'] if 'weight' in args else None
    resolution = args['resolution'] if 'resolution' in args else 1.0
    threshold = args['threshold'] if 'threshold' in args else 1e-07
    seed = args['seed'] if 'seed' in args else None

    node2cluster = community.best_partition(G.to_undirected())
    # 将cluster添加到输入的图中
    for node_id, cluster in node2cluster.items():
        if node_id in G:
            G.nodes[node_id]['cluster'] = cluster
    return (G,)

# GN社团划分算法
@register(input_type=Type.GRAPH, output_type=Type.GRAPH)
@ray.remote
def girvan_newman_communities(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    communities_generator = nx_comm.girvan_newman(G)
    # 获取第一层的社团划分
    communities = next(communities_generator)
    for i, community in enumerate(communities):
        for node_id in community:
            G.nodes[node_id]['cluster'] = i
    return (G,)

# 谱聚类社团发现
@register(input_type=Type.GRAPH, output_type=Type.GRAPH)
@ray.remote
def spectral_clustering_communities(args, output_refs, locators):
    assert {'k'} <= args.keys(), "Missing arguments for spectral_clustering_communities"
    G = fetch(output_refs, locators[0])
    num_clusters = int(args['k'])
    sc = SpectralClustering(n_clusters=num_clusters, affinity='nearest_neighbors')
    # 将节点编号转换为2D数组
    node2cluster = sc.fit_predict(np.array(list(G.nodes())).reshape(-1, 1))

    communities = [[] for _ in range(num_clusters)]

    for node, cluster_label in enumerate(node2cluster):
        communities[cluster_label].append(node)
    # 将社团划分结果保存到图中
    for i, community in enumerate(communities):
        for node_id in community:
            G.nodes[node_id]['cluster'] = i
    return (G,)

#  随机游走社团发现
@register(input_type=Type.GRAPH, output_type=Type.GRAPH)
@ray.remote
def walktrap_communities(args, output_refs, locators):
    assert 'k' in args, "Missing arguments for walktrap_communities"
    G = fetch(output_refs, locators[0])
    k = int(args['k'])

    """
    Parameters:
    -----------
    G : NetworkX graph
    k : int
        Number of clusters to detect.

    Returns:
    --------
    communities : list of lists
        A list of k sublists, where each sublist contains the nodes in a community.
    """
    A = nx.to_numpy_array(G)
    n = len(A)
    D = np.diag(np.sum(A, axis=1))
    L = np.subtract(D, A)
    eigvals, eigvecs = np.linalg.eigh(L)
    U = eigvecs[:, :k]
    U_norm = np.divide(U, np.linalg.norm(U, axis=1)[:, None])
    partition = dict()
    for i in range(n):
        distances = np.linalg.norm(U_norm - U_norm[i], axis=1)
        closest_k = np.argsort(distances)[:k]
        subgraph = G.subgraph(closest_k)
        components = list(nx.connected_components(subgraph))
        part = [0] * n
        for j, comp in enumerate(components):
            for node in comp:
                part[node] = j
        partition[i] = tuple(part)

    communities = [[] for _ in range(k)]
    for node, community_labels in partition.items():
        communities[community_labels[node]].append(node)
    for i, community in enumerate(communities):
        for node_id in community:
            G.nodes[node_id]['cluster'] = i
    return (G,)


# 计算连通图的网络直径
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def network_diameter(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    if isinstance(G, nx.DiGraph) or isinstance(G, nx.MultiDiGraph):
        is_connected = nx.is_strongly_connected(G)  # 判断有向图是否强连通
    else:
        is_connected = nx.is_connected(G)  # 判断无向图是否连通
    assert is_connected, "The graph is not connected, and it is recommended to use the maximum connectivity component for preprocessing."
    g = ig.Graph.from_networkx(G)
    result_dict = {"network_diameter": float(ig.Graph.diameter(g))}
    return (result_dict,)

#计算网络平均度
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def avg_degree(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    # 计算网络的平均度
    average_degree = sum(dict(G.degree()).values()) / len(G)
    result_dict = {"network_avg_degree": average_degree}
    return (result_dict,)

#聚集系数计算
@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def clustering(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    res = dict()
    res["clustering"] =nx.transitivity(G)
    return (res,)

"""节点与边的统计"""

@register(input_type=Type.GRAPH, output_type=Type.DICT)
@ray.remote
def network_node_edge(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    res = dict()
    res["nodeNumber"] = nx.number_of_nodes(G)
    res["edgeNumber"] = nx.number_of_edges(G)
    return (res,)

# 得到图的最大连通子图
@register(input_type=Type.GRAPH, output_type=Type.GRAPH)
@ray.remote
def max_connected_component(args, output_refs, locators):
    G = fetch(output_refs, locators[0])
    largest = max(nx.connected_components(G), key=len)
    return (G.subgraph(largest),)


import time
@register()
@ray.remote
def sleep_test(args, output_refs, locators):
    print("sleep now!")
    time.sleep(60)
