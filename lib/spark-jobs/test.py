import imp
from textwrap import indent
from time import sleep
from turtle import color

from matplotlib.font_manager import json_load
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
import json
import numpy as np
import yake
import requests
import re
import scipy
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from scipy.signal import argrelextrema
from tabulate import tabulate

import numpy as np
from math import factorial

news_source_com = "https://www.kommersant.ru/archive/list/77/day/{}"
news_source_ved = "https://www.vedomosti.ru/archive/{}"

conf = SparkConf().setAppName("updateTest")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
spark = SQLContext(sc)

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    
    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')


def constructQuery(page_name):
    #TODO: will not work for more than a year data, must be reworked
    start = 0
#    q = """
#        {
#          "from" : 0,
#          "size" : 10000,
#          "query" : {
#              "query_string" : {
#                  "query" : "Page.keyword": "{}",
#                  "default_field" : "Page"
#              }
#         }
#        }
#        """.format(page_name)
    q = '{ "from" :  0, "size" : 10000, "query": { "query_string": { "query": "Page.keyword: \\"' + page_name + '\\"", "default_field": "Page" } } }'
    return q

def striphtml(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)

def getWikiText(page_name):
    r = requests.get('https://ru.wikipedia.org/wiki/{}'.format(page_name))
    return striphtml(r.text)

def getKeyWords(page_name):
    text = getWikiText(page_name)
#    kw_extractor = yake.KeywordExtractor()
    language = "ru"
    max_ngram_size = 3
    deduplication_threshold = 0.1
    numOfKeywords = 20
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    print(keywords)
    return keywords

def findGrowth(data_per_day, coef = 0.3):

    maxs = argrelextrema(data_per_day, np.greater)[0]
    mins = argrelextrema(data_per_day, np.less)[0]

    periods = []

    if mins[0] > maxs[0]:
        maxs = maxs[1:]

    if len(mins) > len(maxs):
        mins = mins[0:-1]

    for i in range(len(mins)):
        cur_coef = data_per_day[maxs[i]] - data_per_day[mins[i]]
        if cur_coef > coef:
            periods.append((mins[i], maxs[i], cur_coef))

    return periods

def formDatesArray(periods, dates):
    days = []
    for period in periods:
        new_days = []
        for new_day in dates[period[0] - 3 : period[1] + 3]:
            new_days.append(new_day[:10])
        days.append(new_days)
    return days

def findKeyWords(href, name, keywords):
    res = []
    print(name)
    for keyWord in keywords:
        if name.find(keyWord[0]) != -1:
            res.append(keyWord[0])
    return res

def getHtmlPage(link):
    count = 0
    while count < 10:
        r = requests.get(link)
        if r.status_code == 200:
            break
        sleep(1)
        count += 1

    if count >= 10:
        print("Failed to get data for date {}, {}", r, link)
        return None

    return r


def getFromComersant(period, keywords, coef):
    news = []
    for date in period:
        r = getHtmlPage(news_source_com.format(date))

        if r == None:
            continue

        res = re.findall(r"\<a href\=\"\/doc\/.*uho__link uho__link\-\-overlay\".*\<\/a\>", r.text)

        for link in res:
            href = "https://www.kommersant.ru/{}".format(link[9:20])
            name = re.search("(?<=\>).*(?=<)", link)[0]
            words = findKeyWords(href, name, keywords)
            if len(words) != 0:
                news.append((date, name, words, coef, href))
    return news

def getFromVedomosti(period, keywords, coef):
    news = []
    for date in period:
        r = getHtmlPage(news_source_ved.format(date.replace('-','/')))

        if r == None:
            continue

        res = re.search("(?<=documents\:)\[\{.*?\}\](?=\,)", str(r.text))[0]

        res = res.replace("id:", "\"id\":").replace("title:", "\"title\":").replace("url:", "\"url\":").replace("\\", "/").replace("u002F", '')
        json_res = json.loads(res)
        for obj in json_res:
            href = "https://www.vedomosti.ru/{}".format(obj["url"])
            name = obj["title"]
            words = findKeyWords(href, name, keywords)
            if len(words) != 0:
                news.append((date, name, words, coef, href))
    return news


def getNewsTopics(dates_arr, keywords, periods_arr):
    news_res = []
    for dates, period in zip(dates_arr, periods_arr):
        news_res.append(getFromComersant(dates, keywords, period[2]) + getFromVedomosti(dates, keywords, period[2]))
    return news_res

page = "Кабо-Верде"

es_read_conf = {
    # specify the node that we are sending data to (this should be the master)
    "es.nodes" : 'localhost',
    # specify the p ort in case it is not the default port
    "es.port" : '9200',
    # specify a resource in the form 'index/doc-type'
    "es.resource" : 'hadoop-test',
    "es.scroll.size" : "10000",
    "es.query" : constructQuery(page),
}


es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_read_conf)


data = es_rdd.values().sortBy(lambda log: log["Date"])
visits = np.array(data.map(lambda log: log["Visits"]).collect(), dtype=int)
times = data.map(lambda log: log["Date"]).collect()
days = [times[i * 24] for i in range(len(times) // 24)]
visits_per_day = np.array([visits[i * 24:(i + 1) * 24].sum() for i in range(len(visits)//24)])
visits_per_week = np.array([visits_per_day[i * 7:(i + 1) * 7].sum() for i in range(len(visits_per_day)//7)])
keywords = getKeyWords(page)
print(keywords)

savitzky = savitzky_golay(visits_per_day, 15, 3)

#normalization
savitzky = (savitzky - np.min(savitzky)) / (np.max(savitzky) - np.min(savitzky))


periods = findGrowth(savitzky)
dates = formDatesArray(periods, days)
print(dates)
res = getNewsTopics(dates, keywords, periods)

final_res = []
for per in res:
    final_res += per

print(tabulate(final_res, ["Date", "Title", "Keywords", "Coef", "Link"], tablefmt="grid"))

plt.plot(range(len(savitzky)), savitzky)

for period in periods:
    plt.plot(range(period[0],period[1]), savitzky[period[0]:period[1]], color="red")

plt.ylabel('visits')
plt.show()
