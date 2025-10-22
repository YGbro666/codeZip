class baseException(Exception):
    def __init__(self):
        print("捕捉到异常了")

    def exceptionCreate(self):
        a = 1
        assert a > 1, "创建一个简单的异常"

class functionException(baseException):
    "函数运行错误异常"
    def __init__(self, functionName):
        super(functionException, self).__init__()
        self.name = functionName
    def __str__(self):
        print("异常信息为: " + "组件 {name} 运行过程中出现错误".format(name = self.name))
        return "组件 {name} 运行过程中出现错误".format(name = self.name)


class storgeException(baseException):
    def __init__(self, path, name):
        super(storgeException, self).__init__()
        self.path = path
        self.name = name
    def __str__(self):
        print("异常信息为: " + "组件 {name} 运算结果在保存过程中出现错误，保存的地址为{path}".format(name = self.name, path = self.path))
        return "组件 {name} 运算结果在保存过程中出现错误，保存的地址为{path}".format(name = self.name, path = self.path)
