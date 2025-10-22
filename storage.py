import ray
import hdfs
import pymongo
from utils import adjust_the_output_to_json
from bson.objectid import ObjectId
import json
from base_exception import storgeException
import time
import pandas as pd
from io import StringIO


class HDFS():
    def __init__(self, url="http://10.92.64.241:9870"):
        self.client = hdfs.Client(url)
        self.path = "/bdap/students"
        # self.path = "/test_run"

    def wait_and_save_in_hdfs_node(self, key, node, output_ref, name):
        user_id, expId = key.split('-')
        # user_id, expId, number = key.split('-')
        # user_id+=number
        output = {}
        output["status"] = output_ref["status"]
        # 节点执行时间
        output["time"] = output_ref["time"]
        if output_ref["status"] == "error":
            output["node_output"] = ""
        else:
            for index, item in enumerate(ray.get(output_ref["node_outputs"])):
                output["node_output"] = adjust_the_output_to_json(item, name)
                try:
                    with self.client.write(self.path + "/{}/.raytmp/{}/{}/{}.json".format(user_id, expId, node, index),
                                           encoding='UTF8',
                                           overwrite=True) as writer:
                        # 关于最终结果的保存格式问题，是将"保存为\"还是直接保存
                        writer.write(json.dumps(output))
                except:
                    output_ref["status"] = "error"
                    output["node_output"] = ""
                    raise storgeException(self.path + "/{}/.raytmp/{}/{}/{}.json".format(user_id, expId, node, index),
                                          name)

            # output["node_output"] = ','.join(
            #     [adjust_the_output_to_json(item) for item in ray.get(output_ref["node_outputs"])])

    def load_csv_from_hdfs(self, hdfs_path):
        with self.client.read(hdfs_path) as fs:
            content = fs.read()
        return content.decode('utf-8-sig')


    def load_cora_from_hdfs(self):
        nodePath = "/bdap/data/cora.content"
        edgePath = "/bdap/data/cora.cites"
        with self.client.read(edgePath) as fs:
            cites = fs.read()
        cites = str(cites, "utf-8")
        cora_cites = pd.read_csv(StringIO(cites), sep='\t', header=None)
        with self.client.read(nodePath) as fs:
            content = fs.read()
        content = str(content, "utf-8")
        cora_content = pd.read_csv(StringIO(content), sep='\t', header=None)
        return cora_content, cora_cites

    def get_full_path(self,path):
        full_path = path.replace('hdfs://bdap-cluster-01:8020', '')
        return full_path

    def load_edges_from_csv(self, edges_path):
        edge_content = self.load_csv_from_hdfs(self.get_full_path(edges_path))
        return edge_content

    # 读取节点属性文件
    def load_node_attributes_from_csv(self, node_attributes_path):
        node_attributes_content = self.load_csv_from_hdfs(self.get_full_path(node_attributes_path))
        df = pd.read_csv(StringIO(node_attributes_content), header=None)
        attributes = {}
        for _, row in df.iterrows():
            node_id = row[0]
            attributes[node_id] = set(row[1:])
        return attributes



class Mongo():
    def __init__(self, url="mongodb://bdapadmin:bdapadmin@bdap-cluster-01:27017/bdap_info?maxPoolSize=256"):
        self.client = pymongo.MongoClient(url)

    def fetch_job_info(self, user_id, exp_id):
        ray_collection = self.client.get_database("bdap_info").get_collection("experiment")
        job_info = ray_collection.find_one({'userId': user_id, '_id': ObjectId(exp_id), 'serviceType': 'raygraph'})
        if job_info is None:
            raise ValueError("Work flow does not exists.")
        return job_info
