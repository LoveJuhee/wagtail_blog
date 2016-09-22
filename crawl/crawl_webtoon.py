from bs4 import BeautifulSoup
import requests
import os


# parameter : 크롤링할 웹툰의 URL주소
# output    : 경로 : 웹툰 제목 / 에피소드 순서 /    파일명 : 1.jpg ~ 이미지개수.jpg 
def crawl_naver_webtoon(episode_url):
    response  = requests.get(episode_url)
    dom = BeautifulSoup(response.text, 'html.parser')

    webtoon_title = dom.select('.comicinfo h2')[0].text.split()[0]
    episode_title = dom.select('.tit_area h3')[0].text.split('화')[0]
    image_lists = dom.select('.wt_viewer img')
    
    for idx, image in enumerate(image_lists):
        image_file_url  = image.attrs.get('src', '')
        image_dir_path  = os.path.join('/home/giftbott/data/webtoon/', webtoon_title, episode_title)
        image_save_path = os.path.join(image_dir_path, str(idx + 1) + '.jpg')

        if not os.path.exists(image_dir_path):
            os.makedirs(image_dir_path)           

        headers = {'Referer': episode_url}
        image_data = requests.get(image_file_url, headers=headers).content

        with open(image_save_path, 'wb') as f:
            f.write(image_file_data)

    print(webtoon_title + " - " + episode_title + "  Download complete ") 


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('다운로드할 웹툰의 URL을 입력하세요.  ex) python crawl_webtoon.py [URL]')
    else:
        crawl_naver_webtoon(sys.argv[1])
