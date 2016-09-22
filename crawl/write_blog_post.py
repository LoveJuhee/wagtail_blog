# -*- coding: utf8 -*-

from selenium import webdriver
import sys, dbconn


def write_blog_post(title, body):
    delay = 1

    try:
        #driver = webdriver.Firefox()
        driver = webdriver.PhantomJS()
        driver.implicitly_wait(delay)     
        driver.get('http://naver.com')
        driver.implicitly_wait(delay)     
    except:
        print('Firefox 실행 실패')
        sys.exit(1)

    try:
        username = driver.find_element_by_css_selector('#id')
        password = driver.find_element_by_css_selector('#pw')

        username.send_keys('itperson')
        password.send_keys(dbconn.NAVER_PW)

        password.submit()
        driver.implicitly_wait(delay)
    except:
        print("로그인 되어 있음!")
    finally:
        driver.get('http://blog.naver.com/itperson')
        driver.implicitly_wait(delay)

    try:
        # 블로그 글쓰기 버튼 실행
        driver.switch_to_frame("mainFrame")   
        driver.implicitly_wait(delay)
        write_button = driver.find_element_by_css_selector('div#post-admin a')
        write_button.click()
        driver.implicitly_wait(delay)   

        # 제목, 본문 입력
        driver.switch_to_frame("se_canvas_frame")
        driver.implicitly_wait(delay)

        title_area = driver.find_element_by_css_selector('div.se_textView textarea')
        driver.implicitly_wait(delay)
        title_area.clear()
        title_area.send_keys(title)

        body_area = driver.find_element_by_css_selector('div.se_editView div.se_textView>div>div')
        driver.implicitly_wait(delay)
        body_area.clear()
        body_area.send_keys(body)
    except:
        print('글 쓰기 중 에러 발생')
        driver.quit()
        sys.exit(1)

    try:
        driver.switch_to_default_content()   

        # 블로그 상단 발행 버튼
        publish_1 = driver.find_element_by_id('se_top_publish_btn')
        publish_1.click()
        
        # 발행 버튼 클릭 시에 나오는 발행 버튼
        publish_2 = driver.find_element_by_css_selector('div.button_wrap button')
        driver.implicitly_wait(delay)
        publish_2.click()
    except:
        print('포스트 발행 중 에러 발생')
    finally:
        driver.quit()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage : python auto_write_post [title_text] [body_text]")
        sys.exit(1)
    else:
        add_post(sys.argv[1], sys.argv[2])
