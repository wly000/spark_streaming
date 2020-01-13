#coding=UTF-8
import random
import time

url_paths = [
    "class/112.html",
    "class/113.html",
    "class/114.html",
    "class/115.html",
    "class/116.html",
    "class/117.html",
    "learn/112.html",
    "list/112.html",
]

ip_slices = [132,156,129,128,168,61,22,34,67,60,10,0,12,87,67,99,133,56,156,23]

http_refer = [
    "http://www.baidu.com/s?wd={query}",
    "http://www.sougou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "http://search.yahoo.com/search?p={query}",
]

search_keyword = [
    "Spark SQL实战",
    "Hadoop实战",
    "Storm实战",
    "Spark Streaming实战",
    "大数据面试",
]

status_code = ["200","404","500"]

def sample_url():
    return random.sample(url_paths, 1)[0]

def sample_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join([str(item) for item in slice])

def sample_referer():
    if random.uniform(0, 1) > 0.5:
        return "_"

    refer_str = random.sample(http_refer, 1)
    query_str = random.sample(search_keyword, 1)
    return refer_str[0].format(query=query_str[0])

def sample_status_code():
    return random.sample(status_code, 1)

def generate_log(count = 10):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    with open("/home/hadoop/data/project/logs/access.log", "w+") as f:
        while count >= 1:
            query_log = "{locaTime}\t{url}\t{ip}\t{refer}\t{statusCode}" \
                .format(url=sample_url(), ip=sample_ip(), refer=sample_referer(), statusCode=sample_status_code(), locaTime=time_str)
            print(query_log)
            f.write(query_log + "\n")
            count -= 1

if __name__ == '__main__':
    generate_log(10)