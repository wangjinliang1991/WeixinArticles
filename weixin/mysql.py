import pymysql

from weixin.config import *


class MySQL():
    def __init__(self, host=MYSQL_HOST, username=MYSQL_USER, password=MYSQL_PASSWORD, port=MYSQL_PORT,
                 database=MYSQL_DATABASE):
        """
        MySQL初始化
        :param host: 用于指定请求资源的主机IP和端口号，内容为请求URL的原始服务器或网关位置
        :param username:
        :param password:
        :param port:
        :param database:
        """
        try:
            # connect()方法声明一个MySQL连接对象db
            self.db = pymysql.connect(host, username, password, database, charset='utf8', port=port)
            # 连接成功调用cursor()方法获得MySQL的操作游标，利用游标执行SQL语句
            self.cursor = self.db.cursor()
        except pymysql.MySQLError as e:
            print(e.args)

    def insert(self, table, data):
        """
        插入数据
        :param table:
        :param data:
        :return:
        """
        keys = '.'.join(data.keys())
        values = '.'.join(['%s'] * len(data))
        # 构造SQL语句，value值 格式化%s实现，再用统一的元祖传到execute()方法里
        sql_query = 'insert into %s (%s) values (%s)'%(table, keys, values)
        try:
            self.cursor.execute(sql_query, tuple(data.values()))
            # commit是真正将语句提交到数据库执行的方法
            self.db.commit()
        except pymysql.MySQLError as e:
            print(e.args)
            # 异常处理，执行失败，用rollback()执行数据回滚，事务机制确保数据一致性
            self.db.rollback()

