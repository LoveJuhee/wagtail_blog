# -*- coding: utf8 -*-


"""
# 주요기능 : 직방 매물 정보 크롤링
# 상세기능 : 원하는 지역의 매물ID 를 가진 URL 을 입력받아 각 매물별 상세 정보를 csv 파일 및 jpg 파일로 저장
# 추가계획 : 현재 계획 없음
# 최종수정 : 2016.10.10
"""

from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import sys
import re
import os


class crawl_zigbang_info:
    # 수행기능 : 특정 지역 아이템 리스트를 가진 URL 링크를 입력하면 각 방에 대한 정보를 csv 파일 및 jpg 파일로 변환하여 저장
    # url형식 : https://api.zigbang.com/v1/items?detail=true&item_ids=6128393&~~~&~~~&~~~
    def crawl_items(self, zigbang_url):
        response = requests.get(zigbang_url)
        init_data = json.loads(response.text)

        rooms_info = init_data.get('items')
        self.save_info_to_csv(rooms_info)
        
        pattern = r'[0-9]{7}'
        item_ids = re.findall(pattern, zigbang_url)
        self.save_image(item_ids)

    # 수행기능 : 각 매물에 대한 정보를 컬럼별로 나누어 '월.일_순서.csv' 파일로 저장
    # 파라미터 : 리스트 ( 각 매물에 세부 정보 )
    def save_info_to_csv(self, rooms_info):
        df = pd.DataFrame(
            columns=["제목","룸 타입", "보증금/월세", "관리비", "부가옵션", "가까운 역", "건물층수",\
                     "엘리베이터", "집 주소", "입주일", "업자이름", "업자번호", "대표번호", "업자주소",\
                     "오픈상태", "현재상태", "비밀메모", "부가설명"]
            )
        
        for idx, item in enumerate(rooms_info):
            room = item.get('item')
            
            df.loc[idx] = [
                room['title'],    # 제목
                room['building_type'] + ' / ' + room['room_type'],    # 룸타입
                str(room['deposit']) +  ' / ' + str(room['rent']),    # 보증금/월세
                room['manage_cost'],    # 관리비
                room['options'],        # 부가옵션
                room['near_subways'],   # 가까운 역 
                str(room['floor']) + ' / ' + str(room['floor_all']),  # 건물층수
                room['elevator'],       # 엘리베이터
                str(room['address1']) + ' ' + str(room['address2']),  # 집 주소
                room['movein_date'],    # 입주일
                room['user_name'],      # 업자이름
                room['user_mobile'],    # 업자번호
                room['agent_phone'],    # 대표번호
                room['agent_address1'], # 업자주소
                room['is_status_open'], # 오픈상태
                room['status'],         # 현재상태
                room['secret_memo'],    # 비밀메모
                room['description'].replace("\n\n","\n"),  # 부가설명
            ]

        count = 1
        currentTime = datetime.now()
        while True:
            csv_path = os.path.join('/home/giftbott', 'data', 'zigbang')
            file_name = str(currentTime.month) + '.' + str(currentTime.day) + '_'
            file_name = file_name + str(count) + '.csv'
            csv_file = os.path.join(csv_path, file_name)
            if os.path.exists(csv_file):
                count += 1
            else:
                break
        
        df.to_csv(csv_file)
    
    # 수행기능 : 방 이미지를 '원하는 경로/방id/번호.jpg' 에 저장
    # 파라미터 : 리스트 ( 직방 각 매물에 대한 ID 리스트 )
    def save_image(self, item_ids):
        for id in item_ids:
            detail_url = 'https://www.zigbang.com/items1/{id}'.format(id = id)
            img_path = os.path.join('/home/giftbott', 'data','zigbang', id)

            # 디렉토리가 있으면 이미 이미지가 저장된 상태이므로 pass
            if not os.path.exists(img_path):
                os.makedirs(img_path)

                detail_response = requests.get(detail_url)
                dom = BeautifulSoup(detail_response.content, 'html.parser')
                img_lists = dom.select('.bxslider img')

                for idx, image in enumerate(img_lists):
                    img_name = os.path.join(img_path, (str)(idx + 1) + '.jpg')

                    with open(img_name, 'wb') as f:
                        f.write(requests.get(image['src']).content)

                        
def crawl(zigbang_url):
    crawl = crawl_zigbang_info()
    crawl.crawl_items(zigbang_url)
