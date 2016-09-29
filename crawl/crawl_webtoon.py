# -*- coding: utf8 -*-


"""
# 주요기능 : 네이버 웹툰 이미지 크롤러
# 상세기능 : 원하는 웹툰의 최신 에피소드 번호와 DB에 저장된 번호를 비교해 이후의 모든 에피소드 이미지를 서버에 웹툰/에피소드/파일.jpg로 저장
# 파라미터 : 1. (필수 파라미터)  (DB에 없는 웹툰) 웹툰의 titleid, (DB에 이미 있는 경우) 웹툰명 또는 titleid 입력
#            2. (생략 가능, 기본값 0) 가져올 마지막 에피소드 번호 , 1화부터가 아닌 최신 화부터 지정한 번호까지 역순으로 크롤링한다.
#            3. (생략 가능, 기본값 None) "only" 라는 값이 들어왔을 경우, 2번 파라미터에 해당하는 번호의 에피소드 하나만 가져온다.
#
# 추가계획 : 1. 지정된 에피소드 하나만 가져오는 기능 추가,  2. 다음 웹툰 크롤링 추가,  3. 블로그 연결,  4. cron 으로 요일별 웹툰 자동 실행
# 최종수정 : 2016.09.29
"""


from bs4 import BeautifulSoup
import requests
import sys
import os
import re
import psycopg2
import dbconn
# dbconn 은 DB 접속 비밀번호가 저장된 개인 세팅파일


class crawl_naver_webtoon:
    _webtoon_title = ""
    _webtoon_titleid = ""

    def __init__(self, user_input_webtoon_title, user_input_assigned_no=0):
        if user_input_webtoon_title.isdigit():
            self._webtoon_titleid = user_input_webtoon_title
        else:
            self._webtoon_title = user_input_webtoon_title
        self._assigned_no = user_input_assigned_no

    def db_connect(self):
        self.connect = dbconn.connect('crawldb')
        self.cursor = self.connect.cursor()

    def db_close(self):
        self.connect.close()

    # 지정한 웹툰의 정보가 DB에 있는지 체크
    # 반환값 : (DB에 있으면) 튜플 (1. titleid , 2. latest_ep_no) , (DB에 없으면) NULL
    def check_webtoon_in_db(self):
        query = "select titleid, latest_ep_no from tb_cr_webtoon " + \
                "where titleid = '{titleId}' or webtoon_title = '{webtoon_title}' LIMIT 1;"\
                .format(titleId=self._webtoon_titleid, webtoon_title=self._webtoon_title)
        self.cursor.execute(query)
        db_result = self.cursor.fetchone()
        return db_result

    # 수행기능 : 지정 웹툰의 모든 에피소드 링크주소 값을 반환. DB에서 번호를 불러왔거나 따로 지정한 경우, 해당 에피소드 번호까지만 반환
    # 파라미터 : 1. 웹툰 타이틀id,  2. DB에 저장되어 있는 최신 에피소드 번호 혹은 따로 지정한 번호 (생략할 경우 디폴트 0)
    # 반환결과 : 1. 리스트( 인덱스[0]  : 튜플 (웹툰명, 최신 에피소드 번호, 최신 에피소드 타이틀),
    #                       인덱스[1:] : 튜플 (에피소드 번호, 에피소드별 링크주소) )
    def get_episode_lists(self, titleid, latest_db_no=0):
        HOST = 'http://comic.naver.com'
        has_nextpage = True
        page = 1
        ep_no_pattern = r"[0-9]+"
        wrong_url_pattern = r"comic.naver.com/main.nhn"

        # 다음 페이지 버튼이 없을 때까지 반복하며 모든 에피소드 데이터 저장
        while has_nextpage:
            url = HOST + '/webtoon/list.nhn?titleId={titleid}&page={page}'.format(
                titleid=titleid,
                page=page
            )

            try:
                response = requests.get(url)
                response.raise_for_status()
            except:
                print(url + "에 연결 실패. 크롤링 시도 중단.")
                sys.exit(1)

            if re.search(wrong_url_pattern, response.url):
                print("잘못된 titleid를 입력하셨습니다. 크롤링 시도 중단.")
                sys.exit(1)

            dom = BeautifulSoup(response.content, 'html.parser')
            title_lists = dom.select('table.viewList td.title a')

            # 에피소드 번호 비교 후 신규 에피소드가 없을 경우 종료
            latest_ep_no = (int)(
                re.findall(
                    ep_no_pattern,
                    title_lists[0].attrs.get('href'))[1]
            )
            if (int)(latest_db_no) >= latest_ep_no:
                print("신규 에피소드가 없습니다.")
                return False

            if page <= 1:
                webtoon_title_name = dom.select('.comicinfo h2')[
                    0].text.split()[0]
            latest_ep_title = title_lists[0].text

            episode_lists = []
            episode_lists.append(
                (webtoon_title_name, latest_ep_no, latest_ep_title))

            for title in title_lists:
                episode_no = (int)(
                    re.findall(
                        ep_no_pattern,
                        title.attrs.get('href'))[1])
                if (int)(latest_db_no) == episode_no:
                    return episode_lists

                episode_link = HOST + title.attrs.get('href')
                episode_lists.append((episode_no, episode_link))

            page += 1
            has_nextpage = bool(dom.select('div.pagenavigation a.next'))

        return episode_lists

    # 수행기능 : 지정한 에피소드 하나의 링크만 가져온다.
    # 파라미터 : 1. 문자열(타이틀ID), 2. int (가져올 episode 번호)
    # 반환내용 : 1. 리스트( 인덱스[0] : 튜플 (웹툰명,  에피소드 번호, 최신 에피소드 타이틀),
    #                       인덱스[1] : 튜플 (에피소드 번호, 에피소드별 링크주소) )
    def get_only_one_episode(self, titleid, episode_no):
        pass

    # 수행기능 : 해당 페이지의 이미지를 서버에 "웹툰명/에피소드 번호/이미지 번호.jpg" 로 저장
    # 파라미터 : 1. 리스트( 인덱스[0]  : 튜플 (웹툰명, 최신 에피소드 번호, 최신 에피소드 타이틀),
    #                       인덱스[1:] : 튜플 (에피소드 번호, 에피소드별 링크주소) )
    def get_webtoon_image(self, episode_lists):
        webtoon_title_name = episode_lists[0][0]

        for episode_no, episode_link in episode_lists[1:]:
            response = requests.get(episode_link)
            dom = BeautifulSoup(response.text, 'html.parser')
            image_lists = dom.select('.wt_viewer img')

            for idx, image in enumerate(image_lists):
                image_file_url = image.attrs.get('src', '')
                image_dir_path = os.path.join(dbconn.BASE_PATH, webtoon_title_name, (str)(episode_no))
                image_save_path = os.path.join(image_dir_path, str(idx + 1) + '.jpg')

                if not os.path.exists(image_dir_path):
                    os.makedirs(image_dir_path)

                # headers 정보 없으면 403 Forbidden 메시지 받게 됨
                headers = {'Referer': episode_link}
                image_data = requests.get(image_file_url, headers=headers).content

                with open(image_save_path, 'wb') as f:
                    f.write(image_data)

        print(webtoon_title_name + "  Download completed ")

    # 수행기능 : 신규 웹툰인 경우 insert, 기존 웹툰인 경우 최신 에피소드 번호와 제목으로 update 수행
    # 파라미터 : 1. 문자열( DML type(insert, update) Value ),  2. 리스트(웹툰명, 최신 에피소드 번호, 최신 에피소드 타이틀)
    # 컬럼이름 : titleid(PK), latest_ep_no, episode_title, webtoon_title
    def update_db(self, webtoon_in_db, webtoon_info):
        data = []
        if not webtoon_in_db:
            query = "insert into tb_cr_webtoon(titleid, latest_ep_no, episode_title, webtoon_title) " + \
                    "values (%s, %s, %s, %s)"
            data.append(
                (self._webtoon_titleid,
                 webtoon_info[1],
                 webtoon_info[2].replace("'",'`'),
                 webtoon_info[0],
                 ))
        elif webtoon_in_db:
            query = "update tb_cr_webtoon set latest_ep_no = %s, episode_title = %s where titleid = %s"
            data.append(
                (webtoon_info[1],
                 webtoon_info[2],
                 self._webtoon_titleid,
                 ))

        try:
            self.cursor.execute(query, data[0])
            self.connect.commit()
        except psycopg2.DatabaseError as e:
            print('DML execute error %s' % e)
            self.connect.rollback()


# Main Function
# 수행기능 : crawl_naver_webtoon 메소드의 각 기능 호출 및 DB 정보 조회, 수정
# 파라미터 : 1. 문자열 ( 웹툰 제목이나 타이틀ID 정보를 받음),    2. 정수 ( 해당 숫자의 에피소드를 포함한 이전
# 에피소드는 크롤링 대상에서 제외)
def main(user_input_title, user_input_assigned_no=0, the_episode_only=None):
    if the_episode_only == "only":
        get_only_one_episode(user_input_title, user_input_assigned_no)
        sys.exit(0)

    webtoon = crawl_naver_webtoon(user_input_title, user_input_assigned_no)
    try:
        webtoon.db_connect()
        db_result = webtoon.check_webtoon_in_db()
    except psycopg2.DatabaseError as e:
        print("DB 연결 또는 조회 실패 %s" % e)

    if not db_result:
        if webtoon._webtoon_titleid:    # DB에 등록되지 않은 웹툰은 타이틀id 값이 필수
            episode_lists = webtoon.get_episode_lists(
                webtoon._webtoon_titleid, webtoon._assigned_no)
        else:
            print("신규 웹툰은 제목 대신 titleid 가 필요합니다.")
            sys.exit(1)
    else:
        webtoon._webtoon_titleid = db_result[0]
        episode_lists = webtoon.get_episode_lists(webtoon._webtoon_titleid, db_result[1])

    if episode_lists:  # 신규 에피소드가 있는 경우만
        webtoon.get_webtoon_image(episode_lists)
        webtoon.update_db((bool)(db_result), episode_lists[0])

    if webtoon.connect:
        webtoon.db_close()


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print(
            "Usage : python crawl_webtoon.py [webtoon_titleid or webtoon_titlename]")
        sys.exit(1)
