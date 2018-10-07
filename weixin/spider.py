from urllib.parse import urlencode
from pyquery import PyQuery as pq
import requests
from requests import ReadTimeout, Session

from weixin.config import PROXY_POOL_URL, VALID_STATUS, MAX_FAILED_TIME, KEYWORD
from weixin.db import RedisQueue
from weixin.mysql import MySQL
from weixin.request import WeixinRequest


class Spider():
    base_url = 'https://weixin.sougou.com/weixin?'
    keyword = KEYWORD
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cookie': 'CXID=68999D20535A955E54EEB369EEBDAA87; SUID=7D0481DF3565860A5B922DAB00041476; '
                  'SUV=00724ADFDF81047D5B9390FE3CE03520; ad=Ukllllllll2b6ALrlllllVmUX@1lllllTc99Kyllll'
                  '9llllljylll5@@@@@@@@@@; IPLOC=CN5101; ABTEST=0|1536564030|v1; weixinIndexVisited=1; '
                  'SNUID=6AEE6B35EBEE9D9F5957A098EBEC0DF0; sct=1; JSESSIONID=aaaqsTn37HldSeg_akWyw; '
                  'ppinf=5|1538793682|1540003282|dHJ1c3Q6MToxfGNsaWVudGlkOjQ6MjAxN3x1bmlxbmFtZTo0NTol'
                  'RTYlODUlQTIlRTYlODUlQTIlRTYlODUlQTIlRTYlOTclQjYlRTUlODUlODl8Y3J0OjEwOjE1Mzg3OTM2ODJ8cmVm'
                  'bmljazo0NTolRTYlODUlQTIlRTYlODUlQTIlRTYlODUlQTIlRTYlOTclQjYlRTUlODUlODl8dXNlcmlkOjQ0Om85d'
                  'DJsdURabHBHRjJ1TF9vbGtrV01MbTlHWFFAd2VpeGluLnNvaHUuY29tfA; pprdig=YXVgbs0p9dU4aBgDw7V_id'
                  'ljKjCcGiXgeUpafLd_FO65GO0AMS3VWq_ogoKBR7XpAChV9r3DxwwMN_lwgpTwjbT4al7JXyKKOua-q3IoMvfo2KwI1'
                  'sXoNQKlyuxomXov9kuvMJkAHq4x6HCYOtsNhkW92H_acgTIeDo65hnDIbc; sgid=15-37413245-AVu4ININKITuO'
                  '1IBrovHceA; ppmdig=153880606700000019649cd69fcbff1cb91d0c6884906b6b; LSTMV=469%2C259; LCLKINT=5007',
        'Host': 'weixin.sogou.com',
        'Upgrade-Insecure-Requests': '1',
    # 为了对付防盗链，对方服务器会事变header中的Referer是不是自己的，所以我们会在头部中加上Referer
        'Referer': 'https://weixin.sogou.com/weixin?query=%E9%A3%8E%E6%99%AF&type=2&page=17&ie=utf8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    # 初始化Session和RedisQueue MySQL对象，分别执行请求、代理调用、存储要求
    session = Session()
    queue = RedisQueue()
    mysql = MySQL()

    def get_proxy(url):
        """
        从代理池中获取代理
        """
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200:
                print('Get Proxy', response.text)
                return response.text
            return None
        except requests.ConnectionError:
            return None

    def start(self):
        """
        初始化工作
        """
        # 全局更新Headers，使得所有请求都能应用Cookies
        self.session.headers.update(self.headers)
        # 起始URL的构造
        start_url = self.base_url + '?' + urlencode({'query': self.keyword, 'type': 2})
        # 构造WeixinRequest对象，回调函数：请求成功后用parse_index()处理和解析 need_proxy参数执行请求须用代理
        weixin_request = WeixinRequest(url=start_url, callback=self.parse_index, need_proxy=True)
        # 请求加入队列，调度第一个请求
        self.queue.add(weixin_request)

    def parse_index(self, response):
        """
        解析索引页
        :param response: 响应
        :return: 新的响应
        """
        doc = pq(response.text)
        # 获取本页所有的微信文章链接
        items = doc('.news-box .news-list li .txt-box h3 a').items()
        for item in items:
            url = item.attr('href')
            # 构造成WeixinRequest之后yield返回
            weixin_request = WeixinRequest(url=url, callback=self.parse_detail)
            yield weixin_request
        # 获取下一页的链接
        next = doc('#sogou_next').attr('href')
        if next:
            url = self.base_url + str(next)
            # 构造成WeixinRequest之后yield返回
            weixin_request = WeixinRequest(url=url, callback=self.parse_index, need_proxy=True)
            yield weixin_request

    def parse_detail(self, response):
        """
        解析详情页
        :param response: 响应
        :return: 微信公众号文章
        """
        doc = pq(response.text)
        # 提取标题、正文文本、发布日期、发布人昵称、公众号名称,组合成字典返回
        data = {
            'title': doc('.rich_media_title').text(),
            'content': doc('.rich_media_content').text(),
            'date': doc('#publish_time').text(),
            'nickname': doc('#meta_content > span.rich_media_meta.rich_media_meta_text').text(),
            'wechat': doc('#profileBt > #js_name').text()
        }
        yield data
        # 返回之后需要判断类型，字典类型调用mysql对象的insert()方法存入数据库


    def request(self, weixin_request):
        """
        执行请求
        :param weixin_request: 请求
        :return: 响应
        """
        try:
            # 先判断请求是否需要代理，调用Session的send()方法执行请求
            if weixin_request.need_proxy:
                proxy = self.get_proxy()
                if proxy:
                    proxies = {
                        'http': 'http://' + proxy,
                        'https': 'https://' + proxy
                    }
                    return self.session.send(weixin_request.prepare(), timeout=weixin_request.timeout,
                                             allow_redirects=False, proxies=proxies)
                # 请求调用prepare()方法转化为Prepared Request,不重定向，请求超时时间，响应返回
                return self.session.send(weixin_request.prepare(), timeout=weixin_request.timeout, allow_redirects=False)
        except (ConnectionError, ReadTimeout) as e:
            print(e.args)
            return False

    def error(self, weixin_request):
        """
        错误处理
        """
        weixin_request.fail_time = weixin_request.fail_time + 1
        print('Request Failed', weixin_request.fail_time, 'Times', weixin_request.url)
        if weixin_request.fail_time < MAX_FAILED_TIME:
            self.queue.add(weixin_request)

    def schedule(self):
        """
        调度请求，schedule()方法，内部是一个循环，条件：队列不为空
        """
        while not self.queue.empty:
            # 调用pop()方法取出下一个请求，request()方法执行请求
            # 第一次循环结束，while继续执行，队列包含第一页内容的文章详情页请求和下一页的请求，
            # 第二次循环得到的下一个请求是文章详情页的请求，重新调用request()方法获得响应，对应回调函数parse_detail()
            weixin_request = self.queue.pop()
            callback = weixin_request.callback
            print('Schedule', weixin_request.url)
            response = self.request(weixin_request)
            # request()方法得到Response对象的状态码合法判断，调用WeixinRequest的回调函数(parse_index())解析
            if response and response.status_code in VALID_STATUS:
                results = list(callback(response))
                # schedule()方法将返回结果遍历，利用isinstance()方法判断返回结果
                if results:
                    for result in results:
                        print('New Result', result)
                        # 判断类型是否相同
                        if isinstance(result, WeixinRequest):
                            self.queue.add(result)
                        if isinstance(result, dict):
                            self.mysql.insert('articles', result)
                else:
                    self.error(weixin_request)
            else:
                self.error(weixin_request)

    def run(self):
        """
        入口
        :return:
        """
        self.start()
        self.schedule()

if __name__ == '__main__':
    spider = Spider()
    spider.run()

