# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from time import sleep
import requests
import re
import sys
import psycopg2
import dbconn


# 수행기능 : 크롤링한 뉴스 기사 Data를 받아 DB에 저장,
# 파라미터 : 타입 : 리스트 ( 튜플 (1. 키워드, 2. 링크, 3. 제목, 4. 사이트, 5. 등록일자) )
# DB컬럼   : ( keyword , link )(PK) , title, site, post_date
def save_data(data):
    try:
        conn = dbconn.connect('crawldb')                    
    except psycopg2.DatabaseError as e:
        print("DB Connection Failed. Try Again. %s" % e)        
        sys.exit(1)

    cur = conn.cursor() 
    query = """insert into TB_CR_NEWS (keyword, link, title, site, post_date) values(%s, %s, %s, %s, %s)"""
    overlaped_link = []
    
    for i in range(0, len(data)):        
        try:
            cur.execute(query, data[i])            
        except psycopg2.IntegrityError as e1:
            print("중복되는 키워드+링크 조합 존재 %s" % e1)
            overlaped_link.append(data[i])            
        except psycopg2.DatabaseError as e2:
            print('DB Error %s' % e2)
            conn.rollback()
        else:
            conn.commit()            
            
    conn.close()
    print("Commit 완료\n")

    if overlaped_link:
        print("중복되었던 키워드 + 링크")
        for overlaped in overlaped_link:            
            print(overlaped)
        
# 수행기능 : 네이버 뉴스 기사 크롤링
class crawl_naver_news:
    def __init__(self):
        pass
        
    # 수행기능 : 쓰레기값이 섞인 텍스트를 인자로 받아 뉴스 기사 등록일자만 리턴
    # 파라미터 : 기사 등록일자와 관련없는 데이터가 들어간 리스트
    # 반환결과 : 2016.01.01. 형태의 일자
    def find_post_date(self, text_list):
        pattern1 = r'^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.$'   # ex) 2000.01.01.
        pattern2 = r'^[0-9]+(\s)*[초분시]'   # ex) 10 시간
        pattern3 = r'^[0-9](\s)*일'          # ex) 3 일

        date = ""
        for text in text_list:            
            if re.match(pattern1, text):
                date = text
            elif re.match(pattern2, text):
                num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])
                if   '시' in text: value = timedelta(hours=num)
                elif '분' in text: value = timedelta(minutes=num)
                elif '초' in text: value = timedelta(seconds=num)
                    
                date = (datetime.now() - value).strftime('%Y.%m.%d.')
            elif re.match(pattern3, text):
                num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])
                date = (datetime.now() - timedelta(days=num)).strftime('%Y.%m.%d.')
        
        return date
    
    # 수행기능 : 페이지 단위로 기사를 읽어와 제목, LINK, 등록일자를 DB에 저장
    # 파라미터 : 1. 검색할 키워드
    #            2. 페이지 몇 개를 읽어올 것인지 지정, 기본 = 1 페이지 (기사 10개)
    def crawl_news(self, keyword, page_count):
        HOST = "https://search.naver.com"
        data = []
        
        for page in range(1, (10 * (page_count - 1)) + 2, 10):            
            URL = HOST + "/search.naver?where=news&start={page}&query={keyword}".format(
                page = page,
                keyword = keyword
            )

            try:
                response = requests.get(URL)
                response.raise_for_status()
            except:
                print('크롤링 실패. 재시도 바람')
                sys.exit(1)

            dom = BeautifulSoup(response.content, 'html.parser')
            article_lists = dom.select('div.news li dl')            

            for article in article_lists:
                link = article.select_one('dt a')['href']
                title = article.select_one('dt a')['title'].replace("'",'`')
                site = 'Naver'
                post_date = self.find_post_date(
                    article.select_one('dd.txt_inline').text.split(' '))
                data.append( (keyword.replace("'", '`'), link, title, site, post_date) )
        
        save_data(data)
        
# 파라미터 : 1. 사이트 (Naver, Daum, Google)
#            2. 검색 키워드 
#            3. (생략 가능, 기본값 1) 크롤링 해올 페이지 개수
def main(site, keyword = None, page_count = 1):
    if not keyword:
        print("키워드 입력은 필수입니다.")
        
    if site.lower() == 'naver':
        crawling = crawl_naver_news()
        crawling.crawl_news(keyword, page_count)
    elif site.lower() == 'daum':
        pass
    elif site.lower() == 'google':
        pass
    else:
        print("Naver, Daum, Google 중에서 선택해주세요.")
