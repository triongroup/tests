from multiprocessing.dummy import Pool as ThreadPool
from django.core.management.base import BaseCommand
import re
import requests
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from fake_useragent import UserAgent
import random
from car.tasks import add_new_car
import linecache
import sys


class Command(BaseCommand):
    help = 'Daily scrape Otomoto'

    ua = UserAgent()  # From here we generate a random user agent
    proxies = []  # Will contain proxies [ip, port]
    pages = []

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument('-c',
                            action='store',
                            type=int,
                            dest='page_count',
                            default=50,
                            help='How many pages will be scraped throw pagination')

    def handle(self, *args, **options):
        print('START: "%s"' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.main(options['page_count'])
        print('END: "%s"' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('pages: ' + str(len(self.pages)))
		
    @staticmethod
    def PrintException(self):
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

    # Retrieve a random index proxy (we need the index to delete it if not working)
    def random_proxy(self):
        if len(self.proxies) == 1:
            self.get_proxies()
        return random.randint(0, len(self.proxies) - 1)

    def get_listing(self, url, page):
        if page > 1:
            url = 'https://www.otomoto.pl/osobowe/uzywane/?page=' + str(page)
        print('URL=' + url)
        print('PAGE=' + str(page))

        if page == 1:
            refer = 'https://www.olx.pl/'
        elif page == 2:
            refer = 'https://www.otomoto.pl/osobowe/uzywane/'
        else:
            refer = 'https://www.otomoto.pl/osobowe/uzywane/?page=' + str((page - 1))

        headers = {
            'User-Agent': self.ua.random,
            'Referer': refer
        }
        proxy_index = self.random_proxy()
        proxy = self.proxies[proxy_index]
        proxys = {
            'https': proxy['ip'] + ':' + proxy['port'],
        }
        links = []
        try:
            r = requests.get(url, headers=headers, proxies=proxys, timeout=10)

            if r.status_code == 200:
                html = r.text
                soup = BeautifulSoup(html, 'lxml')
                listing_section = soup.find_all(href=re.compile("/oferta/"))
                for link in listing_section:
                    if link['href'].strip() not in links:
                        links.append(link['href'].strip())
        except:
            self.PrintException(self)
            del self.proxies[proxy_index]
            print('send page=' + str(page))
            self.get_listing(url, page)

        return links

    # parse a single item to get information
    def parse_car(self, url):
        if url not in self.pages:
            self.pages.append(url)

        proxy_index = self.random_proxy()
        proxy = self.proxies[proxy_index]
        headers = {
            'User-Agent': self.ua.random,
            'Referer': 'https://www.otomoto.pl/osobowe/uzywane/'
        }
        proxys = {
            'https': proxy['ip'] + ':' + proxy['port'],
        }

        item = {}
        try:
            r = requests.get(url, headers=headers, proxies=proxys, timeout=10, allow_redirects=False)
            sleep(2)

            if r.status_code == 200:
                print('Processing..' + url)
                html = r.text
                soup = BeautifulSoup(html, 'lxml')

                now = datetime.now()
                id_item = 0
                match = re.search(r"ID(\w+)", url)
                if match:
                    id_item = match.group(1)

                item['active'] = 1
                item['domain'] = 'otomoto'
                item['country'] = 'pl'
                item['link'] = url
                item['identifier'] = id_item
                item['date_insert'] = now.strftime('%Y-%m-%d %H:%M:%S')
                item['date_update'] = now.strftime('%Y-%m-%d %H:%M:%S')
                price = soup.select_one('.offer-price__number').text
                price = price[0:-4].strip()
                item['price'] = price.replace(' ', '')
                item['date_create'] = self.get_date(soup.select('.offer-meta__value')[0].text)
                try:
                    location = soup.select('.seller-box__seller-address__label')[0].text
                except:
                    location = ''
                item['location'] = location.strip().lower()
                descr = soup.select('.offer-description > div')[0].text
                item['description'] = ''.join(descr).replace('\n', '<br />').replace('\r\n', '<br /><br />')

                additional = []
                for additionalItem in soup.select('.offer-features__item'):
                    elem = additionalItem.text
                    elem = ''.join(elem).strip()
                    if elem != '':
                        additional.append(elem)
                item['additional'] = ', '.join(additional)

                for param in soup.select("li.offer-params__item"):
                    label_text__extract = param.select('span.offer-params__label')[0].text
                    if 'Oferta od' == label_text__extract:
                        seller = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        seller = ''.join(seller).strip()
                        if seller == 'Firmy':
                            item['seller'] = 2
                        elif seller == 'Osoby prywatnej':
                            item['seller'] = 1
                        else:
                            item['seller'] = 0
                    if 'Wersja' == label_text__extract:
                        item['version'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['version'] = ''.join(item['version']).strip()
                    if 'Stan' == label_text__extract:
                        val = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        val = ''.join(val).strip()
                        if 'Używane' == val:
                            item['used'] = 1
                        else:
                            item['used'] = 0
                    if 'Przebieg' == label_text__extract:
                        item['mileage'] = param.select('div.offer-params__value')[0].text
                        item['mileage'] = ''.join(item['mileage']).strip()
                        item['mileage'] = item['mileage'][0:-2]
                        item['mileage'] = item['mileage'].strip().replace(' ', '')
                        item['mileage'] = int(item['mileage'])
                    if 'VIN' == label_text__extract:
                        item['vin'] = param.select('div.offer-params__value')[0].text
                        item['vin'] = ''.join(item['vin']).strip()
                    if 'Kolor' == label_text__extract:
                        item['color'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['color'] = ''.join(item['color']).strip()
                    if 'Bezwypadkowy' == label_text__extract:
                        val = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        val = ''.join(val).strip()
                        if 'Tak' == val:
                            item['broken'] = 0
                        else:
                            item['broken'] = 1
                    if 'Kraj pochodzenia' == label_text__extract:
                        item['country_init'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['country_init'] = ''.join(item['country_init']).strip().lower()
                    if 'Rok produkcji' == label_text__extract:
                        item['year'] = param.select('div.offer-params__value')[0].text
                        item['year'] = ''.join(item['year']).strip()
                    if 'Pojemność skokowa' == label_text__extract:
                        item['volume'] = param.select('div.offer-params__value')[0].text
                        item['volume'] = ''.join(item['volume'])
                        item['volume'] = item['volume'].strip()[0:-3]
                        volume = item['volume'].strip().replace(' ', '')
                        item['volume'] = round(int(volume) / 1000, 1)
                    if 'Moc' == label_text__extract:
                        item['max_speed'] = param.select('div.offer-params__value')[0].text
                        item['max_speed'] = ''.join(item['max_speed']).strip()[0:-2]
                        item['max_speed'] = item['max_speed'].strip()
                    if 'Typ' == label_text__extract:
                        item['type'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['type'] = ''.join(item['type']).strip()
                    if 'Napęd' == label_text__extract:
                        item['drive_mode'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['drive_mode'] = ''.join(item['drive_mode']).strip().lower()
                    if 'Skrzynia biegów' == label_text__extract:
                        item['gear'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['gear'] = ''.join(item['gear']).strip().lower()
                    if 'Rodzaj paliwa' == label_text__extract:
                        item['fuel'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['fuel'] = ''.join(item['fuel']).strip().lower()
                    if 'Marka pojazdu' == label_text__extract:
                        item['vendor'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['vendor'] = ''.join(item['vendor']).strip()
                    if 'Model pojazdu' == label_text__extract:
                        item['model'] = param.select('div.offer-params__value > a.offer-params__link')[0].text
                        item['model'] = ''.join(item['model']).strip()

                    photos = []
                    for photoItem in soup.select('a.offer-photos-thumbs__link'):
                        img = photoItem['href']
                        img = ''.join(img)
                        photos.append(img)

                    item['photos'] = ', '.join(photos)

                add_new_car.delay(item)

        except:
            self.PrintException(self)
            try:
                del self.proxies[proxy_index]
            except IndexError:
                pass
            self.parse_car(url)

    def get_date(self, param):
        month_map = {
            'stycznia': '01',
            'lutego': '02',
            'marca': '03',
            'kwietnia': '04',
            'maja': '05',
            'czerwca': '06',
            'lipca': '07',
            'sierpnia': '08',
            'września': '09',
            'października': '10',
            'listopada': '11',
            'grudnia': '12',
        }

        time, full_date = param.split(", ")
        day, month, year = full_date.split(" ")

        return year + '-' + month_map[month] + '-' + day + ' ' + time

    def get_proxies(self):
        headers = {'User-Agent': self.ua.random}
        # Retrieve latest proxies
        proxies_req = requests.get('https://www.sslproxies.org/', headers=headers)
        proxies_doc = proxies_req.text

        soup = BeautifulSoup(proxies_doc, 'html.parser')
        proxies_table = soup.find(id='proxylisttable')

        # Save proxies in the array
        for row in proxies_table.tbody.find_all('tr'):
            self.proxies.append({
                'ip': row.find_all('td')[0].string,
                'port': row.find_all('td')[1].string
            })

    def main(self, cnt):
        self.get_proxies()
        for i in range(1, cnt + 1):
            cars_links = self.get_listing('https://www.otomoto.pl/osobowe/uzywane/', i)
            pool = ThreadPool(10)
            pool.map(self.parse_car, cars_links)
            pool.terminate()
            pool.close()
            pool.join()
        print('DONE!')
