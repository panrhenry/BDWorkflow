# coding=utf-8
"""
dataflow XML解析工具类
@author jiangbing
"""
import xml.dom.minidom as xml
import multiprocessing


class DataflowXmlUtil:
    """dataflow XML解析工具类"""
    def __init__(self, str_xml, allow_parallel, parallel_num):
        self.dom = xml.parseString(str_xml)
        self.root = self.dom.documentElement
        # 节点结束标记
        self.end_nodes = []
        # 是否并行
        self.allow_parallel = allow_parallel
        # 并行个数
        self.parallel_num = int(parallel_num)

    def get_attr(self, node, name):
        """获取节点属性值"""
        if node:
            return node.getAttribute(name)
        else:
            return ""

    def get_sub_node(self, node, name):
        """获取子节点列表"""
        if node:
            return node.getElementsByTagName(name)
        else:
            return []

    def get_sub_node_data(self, node, name):
        """获取子节点值"""
        if node:
            sub_node = self.get_sub_node(node, name)
            if sub_node is not None and len(sub_node) > 0 and sub_node[0].firstChild is not None:
                return sub_node[0].firstChild.nodeValue

        return ""

    def get_node_data(self, node):
        """获取节点值"""
        if node:
            return node.firstChild.nodeValue
        else:
            return ""

    def findby_id(self, name, id):
        """根据id获取子节点"""
        if self.root:
            nodes = self.get_sub_node(self.root, name)
            for node in nodes:
                if node.getAttribute("id") == id:
                    return node
        return None

    def deal_end(self, node, callback):
        if node is None:
            return
        callback(self, node)
        targetRef = self.get_attr(node, "targetRef")
        if targetRef:
            targets = targetRef.split(",")
            # if len(targets) > 1:
            #     return
            # callback(self, node)
            for target in targets:
                if target in self.end_nodes:
                    # 等待分支流程执行完毕
                    return
                self.deal_end(self.findby_id("step", target), callback)

    def deal_node(self, obj):
        if obj["node"] is None:
            return
        # 执行节点
        obj["callback"](self, obj["node"])

        targetRef = self.get_attr(obj["node"], "targetRef")
        if targetRef:
            targets = targetRef.split(",")
            if self.allow_parallel == "true":
                if len(targets) > 1:
                    pool = multiprocessing.Pool(processes=self.parallel_num)
                    for target in targets:
                        pool.apply_async(self.deal_end, args=(self.findby_id("step", target), obj["callback"]))
                    pool.close()
                    pool.join()
                    return
            for target in targets:
                if target in self.end_nodes:
                    # 等待分支流程执行完毕
                    return
                self.deal_node({
                    'node': self.findby_id("step", target),
                    'callback': obj["callback"]
                })

    def run(self, callback):
        """执行dataflow XML"""
        steps = self.get_sub_node(self.root, "step")
        # 预处理
        self.pre_deal(steps)
        # 遍历子图
        for index in range(len(self.end_nodes)):
            self.deal_node({
                'node': self.findby_id("step", self.end_nodes[index]),
                'callback': callback
            })

    def pre_deal(self, steps):
        """预处理"""
        self.end_nodes = []
        self.end_nodes.append("0000")
        # 如果节点有多个来源或多个目标节点，将流程图切分为多个子图
        for index, step in enumerate(steps):
            sourceRef = self.get_attr(step, "sourceRef")
            # targetRef = self.get_attr(step, "targetRef")
            if sourceRef:
                if sourceRef.find(",") > -1:
                    self.end_nodes.append(self.get_attr(step, "id"))
            # if targetRef:
            #     if targetRef.find(",") > -1:
            #         self.end_nodes.append(self.get_attr(step, "id"))
        # self.end_nodes.append("9999")
        # tmp = []
        # for key in self.end_nodes:
        #     if key not in tmp:
        #         tmp.append(key)
        # self.end_nodes = tmp
