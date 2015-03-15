__author__ = 'GongLi'

import util
import time
from bs4 import BeautifulSoup
import urllib
from time import mktime
from datetime import datetime
import json
import MySQLdb
import ConfigParser



class Question:

    def __init__(self, name, link_id, focus, answer, add_time, top_answer_number):

        self.name = name
        self.linkID = link_id
        self.focus = focus
        self.answer = answer
        self.addTime = add_time
        self.topAnswerNumber = top_answer_number


    def toInsertSQL(self, db, count):
        print str(count) +':'+ str(self.linkID)

        sql = "INSERT IGNORE INTO QUESTION (NAME, LINK_ID, FOCUS, ANSWER, ADD_TIME, TOP_ANSWER_NUMBER, LAST_VISIT) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        db.execute(sql, (self.name, self.linkID, self.focus, self.answer, self.addTime, self.topAnswerNumber, int(time.time())))



def getFirstPage():
    toUrl = 'http://www.zhihu.com/log/questions'
    content = util.get_content(toUrl, 0)

    return parsePage(content)


def getMoreQuestions(startID, offset=20):

    data = {}
    data['start'] = startID
    data['offset'] = str(offset)
    data['_xsrf'] = 'f476d47f58e70ef838d30d2164d17537'

    data = urllib.urlencode(data)
    content = util.get_content('http://www.zhihu.com/log/questions', 0, data)
    content = json.loads(content)['msg'][1]

    return parsePage(content)


def parsePage(content):
    soup = BeautifulSoup(content)
    questions = soup.findAll('div',attrs={'class': 'zm-item'})

    finalQuestionLinkID = 0


    items = []
    for question in questions:

        questionDetail1 = question.find('h2', attrs={'class', 'zm-item-title'})

        detailQuestion = questionDetail1.find('a')

        linkId = detailQuestion.attrs['href'][10:]
        title = detailQuestion.text

        addTime = question.find('time').text
        addTime = time.strptime(addTime, "%Y-%m-%d %H:%M:%S")
        addTime = datetime.fromtimestamp(mktime(addTime))
        addTimeEpochTime = (addTime - datetime(1970,1,1)).total_seconds()

        xx = Question(title, int(linkId), 0, 0, addTimeEpochTime, 0)

        finalQuestionLinkID = linkId
        items.append(xx)

    return finalQuestionLinkID, items


def main(db, iteration):
    count = 0

    tempLinkedId, questions = getFirstPage()
    for q in questions:
        count += 1
        q.toInsertSQL(db, count)

    for i in range(iteration):
        tempLinkedId, questions = getMoreQuestions(tempLinkedId)

        for q in questions:
            count += 1
            q.toInsertSQL(db, count)


def getDB():
    cf = ConfigParser.ConfigParser()
    cf.read("config.ini")

    host = cf.get("db", "host")
    port = int(cf.get("db", "port"))
    user = cf.get("db", "user")
    passwd = cf.get("db", "passwd")
    db_name = cf.get("db", "db")
    charset = cf.get("db", "charset")
    use_unicode = cf.get("db", "use_unicode")

    db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
    cursor = db.cursor()

    return cursor




if __name__ == '__main__':

    db = getDB()
    main(db, 20)




