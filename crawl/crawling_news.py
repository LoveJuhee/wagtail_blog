# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import sys, requests, re, dbconn
import psycopg2


def find_post_date(text_list):
    pattern1 = r'^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.$'   # ex) 2000.01.01.
    pattern2 = r'^[0-9]+(\s)*[초분시]'   # ex) 10 시간
    pattern3 = r'^[0-9](\s)*일'          # ex) 3 일

    for text in text_list:
        if re.match(pattern1, text):
            return text
        elif re.match(pattern2, text):
            num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])

            if   '시' in text: value = timedelta(hours=num)
            elif '분' in text: value = timedelta(minutes=num)
            elif '초' in text: value = timedelta(seconds=num)

            return (datetime.now() - value).strftime('%Y.%m.%d.')
        elif re.match(pattern3, text):
            num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])

            return (datetime.now() - timedelta(days=num)).strftime('%Y.%m.%d.')



def crawling(keyword, page_count=1):
    try:
        conn = dbconn.connect()
        cur = conn.cursor() 

        for page in range(1, (10 * (page_count - 1)) + 2, 10):
            URL = "https://search.naver.com/search.naver?where=news&start={page}&query={keyword}".format( page = page, keyword = keyword )
                
            try:
                r = requests.get(URL)
                r.raise_for_status()

                dom = BeautifulSoup(r.content, 'html.parser')
                elements = dom.select('div.news li dl')
                data = []

                for element in elements:
                    link = element.select_one('dt a').attrs.get('href','Error')
                    title = element.select_one('dt a').attrs.get('title').replace("'",'`')
                    site = 'Naver'
                    post_date = find_post_date( element.select_one('dd.txt_inline').text.split(' ') )
                    data.append( (keyword.replace("'", '`'), link, title, site, post_date) )
            except:
                print('크롤링 실패. 재시도 바람')
                sys.exit(1)
        

        query = 'insert into TB_CR_NEWS (keyword, link, title, site, post_date) values(%s, %s, %s, %s, %s)'
        try:
            cur.executemany(query, data)
            conn.commit()
            print("커밋 완료")
        except psycopg2.IntegrityError as e1:
            print('Integrity Error %s' %e1)
            if conn:
                conn.rollback()
        except psycopg2.DatabaseError as e2:
            if conn:
                conn.rollback()
            print('DB Error %s' % e2)
        finally:
            if conn:
                conn.close()
    except:
        print("DB연결 실패")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print('키워드를 입력하세요. ex) python crawling_news.py [keyword] [page]')
    elif len(sys.argv) == 2:
        crawling(sys.argv[1])
    else:
        crawling(sys.argv[1], sys.argv[2])
