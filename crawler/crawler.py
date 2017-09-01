import scrapy
import json
import os.path
import pickle
from bs4 import BeautifulSoup
import socket

class FacebookSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://graph.facebook.com/gdacs/feed?access_token=462341000812745|6d28c7ac3ff22a978dceb052f47ff4fd&fields=id,link,created_time']

    def init_socket(self):
        addr = "localhost"
        port = 3001
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((addr, port))

    def send_message(self, msg):
        self.init_socket()
        data = str.encode(msg + "\n")
        self.s.send(data)
        self.close_socket()

    def close_socket(self):
        self.s.close()

    def parse(self, response):
        new_gdacs_url = self.parse_facebook_post(response)
        if (new_gdacs_url is not None):
            yield response.follow(new_gdacs_url, self.parse_gdacs_summary)

    def parse_gdacs_summary(self, response):
        source_code = response.body_as_unicode()
        file_name = 'a.pkl'
        last_response = self.read_pickle(file_name)
        if (last_response is not None and last_response['id']):
            soup = BeautifulSoup(source_code, 'html.parser')
            self.send_message(soup.prettify())
            subsubmenus = soup.find_all('div', { 'class' : 'subsubmenuitem' })
            full_report_link = subsubmenus[1].find('a').get('href')
            yield response.follow(full_report_link, self.parse_gdacs_impact)

    def parse_gdacs_impact(self, response):
        source_code = response.body_as_unicode()
        file_name = 'a.pkl'
        last_response = self.read_pickle(file_name)
        if (last_response is not None and last_response['id']):
            soup = BeautifulSoup(source_code, 'html.parser')
            self.send_message(soup.prettify())

    def read_pickle(self, file_name):
        last_response = None
        if (os.path.isfile(file_name)):
            try:
                with open(file_name, "rb") as file:
                    last_response = pickle.load(file)
            except Exception as e:
                last_response = None
        return last_response

    def parse_facebook_post(self, response):
        file_name = 'a.pkl'
        jsonresponse = json.loads(response.body_as_unicode())['data'][0]
        save_response = True
        if (os.path.isfile(file_name)):
            try:
                with open(file_name, "rb") as file:
                    last_response = pickle.load(file)
                save_response = last_response['id'] != jsonresponse['id']
            except Exception as e:
                return None
        if (save_response):
            with open(file_name, 'wb') as file:
                pickle.dump(jsonresponse, file)
            return jsonresponse['link']
        return None
