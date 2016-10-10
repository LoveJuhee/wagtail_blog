# -*- coding: utf8 -*-


"""
# 주요기능 : 주요 포털사이트 뉴스 카테고리, 지정 키워드에 대한 검색 결과 크롤링
# 상세기능 : 1. crawl 함수 - 지정 키워드에 대한 검색 결과를 사이트/키워드.csv 파일과 DB에 저장
#          2. print_news 함수 - DB에 지금까지 저장한 뉴스 기사 정보 출력
#
# 추가계획 : 1. 구글 키워드 검색, 2. pandas dataframe 적용해서 검색 결과 csv 로 만들기
#          3. db에 저장된 데이터 출력 기능, 4. 지정 메일로 자동 발송
# 최종수정 : 2016.10.09  (수정중)
"""


from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from time import sleep
import requests
import re
import os
import sys
import psycopg2
import dbconn
#dbconn은 개인설정파일


# 수행기능 : 크롤링한 뉴스 기사 Data를 받아 DB에 저장,
# 파라미터 : 타입 : 리스트 ( 튜플 (1. 키워드, 2. 링크, 3. 제목, 4. 언론사, 5. 등록일자, 6. 사이트) )
# DB컬럼  : ( keyword , link )(PK) , title, press, post_date, site
def save_data(data):
    try:
        conn = dbconn.connect('crawldb')                    
    except psycopg2.DatabaseError as e:
        print("DB Connection Failed. Try Again. %s" % e)        
        sys.exit(1)

    cur = conn.cursor() 
    query = "insert into TB_CR_NEWS (keyword, link, title, press, post_date, site)"\
            + "values(%s, %s, %s, %s, %s, %s)"
    overlaped_link = []
    
    for i in range(0, len(data)):        
        try:
            cur.execute(query, data[i])            
        except psycopg2.IntegrityError as e1:
            overlaped_link.append(data[i])  
            conn.rollback()
        except psycopg2.DatabaseError as e2:
            print('DB Error %s' % e2)
            conn.rollback()
        else:
            conn.commit()
            
    conn.close()

    file_path = os.path.join(dbconn.BASE_PATH, 'news', data[0][5])
    file_name = os.path.join(file_path, data[0][0] + '.txt')
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    
    f = open(file_name, 'w')
    
    for idx, news in enumerate(data):
        overlaped_str = ""

        if news in overlaped_link:
            overlaped_str = " - 중복"
        
        index = '[' + str(idx + 1) + ']' + overlaped_str
        f.write(index + '\n')
        f.write('제목 : ' + news[2] + '\n')
        f.write('링크 : ' + news[1] + '\n')
        f.write('언론 : ' + news[3] + '\n')
        f.write('등록 : ' + news[4] + '\n')
        print(index)
        print(news)
        print()
    
    f.close()
        
    if overlaped_link:
        new_data = len(data) - len(overlaped_link)
        print('신규 : ' + str(new_data) + '건 저장.  중복 : ' + str(len(overlaped_link)) + '건 제외')
        
# 수행기능 : 네이버 뉴스 기사 크롤링
class crawl_naver_news:
    NAVER = "https://search.naver.com/search.naver"
    
    # 수행기능 : 쓰레기값이 섞인 텍스트를 인자로 받아 뉴스 기사 등록일자만 리턴
    # 파라미터 : 기사 등록일자와 관련없는 데이터가 들어간 리스트
    # 반환결과 : 2016.01.01 같이 날짜 형태의 문자열
    def find_date_format(self, text_list):
        pattern1 = r'^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.$'   # ex) 2000.01.01.
        pattern2 = r'^[0-9]+(\s)*[초분시]'   # ex) 10 시간 전
        pattern3 = r'^[0-9](\s)*일'         # ex) 3 일 전

        date = ""
        for text in text_list:            
            if re.match(pattern1, text):
                text = text[0:len(text)-1]  # 마지막 . 삭제
                date = text
            elif re.match(pattern2, text):
                num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])
                if   '시' in text: value = timedelta(hours=num)
                elif '분' in text: value = timedelta(minutes=num)
                elif '초' in text: value = timedelta(seconds=num)
                    
                date = (datetime.now() - value).strftime('%Y.%m.%d')
            elif re.match(pattern3, text):
                num = int(text[0:2]) if text[0:2].isdigit() else int(text[0])
                date = (datetime.now() - timedelta(days=num)).strftime('%Y.%m.%d')
        
        return date
    
    # 수행기능 : 페이지 단위로 기사를 읽어와 키워드, 링크, 기사제목, 언론사, 등록일자를 DB에 저장
    # 파라미터 : 1. 검색할 키워드
    #          2. 페이지 몇 개를 읽어올 것인지 지정, 기본 = 1 페이지 (기사 10개)
    def crawl_news(self, keyword, page_count):
        data = []
        
        for page in range(1, (10 * (page_count - 1)) + 2, 10):
            url = self.NAVER + "?where=news&start={page}&query={keyword}".format(
                page = page,
                keyword = keyword
            )

            try:
                response = requests.get(url)
                response.raise_for_status()
            except:
                print('크롤링 실패. 재시도 바람')
                sys.exit(1)

            dom = BeautifulSoup(response.content, 'html.parser')
            article_lists = dom.select('div.news li dl')

            for article in article_lists:
                link_N_title = article.select_one('dt a')
                press_N_date = article.select_one('dd.txt_inline')
                
                link = link_N_title['href']
                title = link_N_title['title'].replace("'",'`')
                press = press_N_date.select_one('._sp_each_source').text
                post_date = self.find_date_format(press_N_date.text.split(' '))
                site = 'naver'
                data.append((
                        keyword.replace("'", '`'),
                        link,
                        title,
                        press,
                        post_date,
                        site,
                    ))
        
        save_data(data)
        
# 수행기능 : Daum 뉴스 기사 크롤링
class crawl_daum_news:
    DAUM = "http://search.daum.net/search"
    
    # 수행기능 : 뉴스 기사 등록일자를 고정된 날짜 형태로 변환
    # 파라미터 : 형태가 고정되지 않은 뉴스기사 등록일자 
    # 반환결과 : 2016.01.01 같이 고정된 날짜 형태의 문자열
    def change_date_format(self, post_date):
        pattern1 = r'^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]$'   # ex) 2000.01.01
        pattern2 = r'^[0-9]+[초분시]'   # ex) 10시간전

        date_format = ""
        if re.match(pattern1, post_date):
            date_format = post_date
        elif re.match(pattern2, post_date):
            if post_date[0:2].isdigit():
                num = int(post_date[0:2])
            else:
                int(post_date[0])
                          
            if   '시' in post_date: value = timedelta(hours=num)
            elif '분' in post_date: value = timedelta(minutes=num)
            elif '초' in post_date: value = timedelta(seconds=num)

            date_format = (datetime.now() - value).strftime('%Y.%m.%d')
        
        return date_format
    
    # 수행기능 : 페이지 단위로 기사를 읽어와 키워드, 링크, 기사제목, 언론사, 등록일자를 DB에 저장
    # 파라미터 : 1. 검색할 키워드
    #          2. 페이지 몇 개를 읽어올 것인지 지정, 기본 = 1 페이지 (기사 10개)
    def crawl_news(self, keyword, page_count):
        data = []
        
        for page in range(1, (10 * (page_count - 1)) + 2, 10):            
            url = self.DAUM + "?w=news&q={keyword}&p={page}".format(
                keyword = keyword,
                page = page,
            )

            try:
                response = requests.get(url)
                response.raise_for_status()
            except:
                print('크롤링 실패. 재시도 바람')
                sys.exit(1)


            dom = BeautifulSoup(response.content, 'html.parser')
            article_lists = dom.select('div#newsColl #clusterResultUL li')

            for article in article_lists:
                link_N_title = article.select_one('.cont_inner a')
                press_N_date = article.select_one('.f_nb').text.split('|')
                
                link = link_N_title['href']
                title = link_N_title.text.replace("'",'`')
                press = press_N_date[1].strip()
                post_date = self.change_date_format(press_N_date[0].strip())
                site = 'daum'
                data.append((
                        keyword.replace("'", '`'),
                        link,
                        title,
                        press,
                        post_date,
                        site,
                    ))
        
        save_data(data)
    
def load_data(keyword, site, post_date):
    try:
        conn = dbconn.connect('crawldb')                    
    except psycopg2.DatabaseError as e:
        print("DB Connection Failed. Try Again. %s" % e)        
        sys.exit(1)

    news_data = []
    cur = conn.cursor() 
    query = "select keyword, link, title, press, post_date, site" +\
            "  from TB_CR_NEWS" +\
            " where keyword like '%%s%'" +\
            "   and site like '%'" +\
            "   and post_date >="
    
    try:
        cur.execute(query)            
    except psycopg2.DatabaseError as e:
        print('Error Occured while Execute Query.\n%s' % e2)
    else:
        try:
            news_data = cur.fetchall()
        except psycopg2.DatabaseError as e:
            print('Error Occured while Fetch Data.\n%s' % e)
    finally:    
        conn.close()
    
    return news_data

# 수행기능 : DB에 저장되어 있는 뉴스 정보 출력
# 파라미터 : 1. DB에 저장된 키워드
#          2. (생략 가능, 기본값:전체) 사이트 지정 (Naver, Daum, Google)
#          3. (생략 가능, 기본값:전체) 기사 등록일 범위
# 출력정보 : 키워드, 링크, 제목, 언론사, 등록일, 사이트
def print_news(keyword, site = None, post_date = None):
#     if site:
#         site = site.lower() if site.isalpha
#     news_data = load_data(keyword, site, post_date)
    
#     print(news_data)
    pass

# 파라미터 : 1. 사이트 (Naver, Daum, Google)
#          2. 검색 키워드 
#          3. (생략 가능, 기본값 1) 크롤링 해올 페이지 개수
def crawl(site, keyword = None, page_count = 1):
    if not keyword:
        print("키워드 입력은 필수입니다.")
        
    if site.lower() == 'naver' or site == '네이버':
        crawling = crawl_naver_news()
        crawling.crawl_news(keyword, page_count)
    elif site.lower() == 'daum' or site == '다음':
        crawling = crawl_daum_news()
        crawling.crawl_news(keyword, page_count)
    elif site.lower() == 'google' or site == '구글':
        pass
    else:
        print("Naver, Daum, Google 중에서 선택해주세요.")
