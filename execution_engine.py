# coding=utf-8
import sys
import ray
from graph_generation import *
from graph_processing import *
from storage import *
from safety import *
from base_exception import *
from utils import rearrange_job_info_format
import time


def main():
    assert len(sys.argv) == 2, "There is no command line args for execution_engine.py"
    key = sys.argv[1]
    # userId, expId, number = key.split('-')
    userId, expId = key.split('-')
    job_info = Mongo().fetch_job_info(userId, expId)

    # define a sequence of execution
    toposorted = topological_sort(job_info)

    # Validate the connected nodes
    validate_type(job_info)
    print("Passed type check")

    new_job_info = rearrange_job_info_format(job_info)

    # Topological sort ensured that the prerequisites are satisfied
    output_refs = dict()
    for node in toposorted:
        output_ref = {"status": "waiting"}
        start_time = int(round(time.time() * 1000))
        name, args, locators = new_job_info[node]['name'], new_job_info[node]['args'], new_job_info[node]['locators']
        output_ref["node_outputs"] = eval("{}".format(name)).remote(args, output_refs, locators)
        try:
            ray.get(output_ref["node_outputs"])
            output_ref["status"] = "ok"
        except:
            output_ref["status"] = "error"
            raise
        # 获取已运行节点的执行信息，若发生错误则更新status
        finish_time = int(round(time.time() * 1000))
        output_ref["time"] = finish_time - start_time
        HDFS().wait_and_save_in_hdfs_node(key, node, output_ref, name)
        if output_ref["status"] == "error":
            raise functionException(name)

        output_refs[node] = output_ref["node_outputs"]

    # print(wait_and_save_in_hdfs(userId+'-'+job_info['title'], output_refs))


if __name__ == "__main__":
    main()
