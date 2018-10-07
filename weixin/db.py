from pickle import dumps, loads

from redis import StrictRedis

from weixin.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_KEY
from weixin.request import WeixinRequest


class RedisQueue():
    def __init__(self):
        """
        初始化Redis
        """
        self.db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)

    def add(self, request):
        """
        向队列添加序列化后的Request
        :param request: 请求对象
        :return: 添加结果
        """
        if isinstance(request, WeixinRequest):
            # 用pickle的dumps()方法序列化request，再用rpush加入队列
            return self.db.rpush(REDIS_KEY, dumps(request))
        return False

    def pop(self):
        """
        取出下一个Request并反序列化
        :return: Reqiest or None
        """
        if self.db.llen(REDIS_KEY):
            # 调用lpop()方法将请求从队列中取出，再用pickle的loads()方法转化为WeixinRequest对象
            return loads(self.db.lpop(REDIS_KEY))
        else:
            return False

    def clear(self):
        self.db.delete(REDIS_KEY)

    def empty(self):
        return self.db.llen(REDIS_KEY) == 0

if __name__ == '__main__':
    db = RedisQueue()
    start_url = 'http://www.baidu.com'
    weixin_request = WeixinRequest(url=start_url, callback='hello', need_proxy=True)
    db.add(weixin_request)
    request = db.pop()
    print(request)
    print(request.callback, request.need_proxy)
