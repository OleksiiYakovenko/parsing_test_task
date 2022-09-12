from bs4 import BeautifulSoup as BS
import requests
from datetime import date
from sqlalchemy import create_engine, Table, Column, String, MetaData


meta = MetaData()

parsed_data = Table('parsed_data', meta,
    Column('image', String(255)),
    Column('title_text', String(255)),
    Column('city', String(255)),
    Column('date_info', String(255)),
    Column('beds', String(255)),
    Column('info', String(1000)),
    Column('curency', String(3)),
    Column('price', String(255))
    )

engine = create_engine("mysql+mysqlconnector://root:yourpasword@localhost/yourdatabase", echo=True)
parsed_data.create(engine)


def get_content(url):
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
              'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    resp = requests.get(url, headers=header)

    if resp.status_code == 200:
        page = BS(resp.text, 'html.parser')
        id_div = page.find_all('div', attrs={'class': 'search-item'})
        for i in id_div:
            image = i.find('img')
            image_data = image['src']

            title = i.find('a', attrs={'class': 'title'})
            title_data = title.text.strip()

            city_div = i.find('div', attrs={'class': 'location'})
            city = city_div.find('span', attrs={'class': ''})
            city_data = city.text.strip()

            date_info = i.find('span', attrs={'class': 'date-posted'})
            date_data = date_info.text.strip(' <')
            if len(date_data) > 10:
                date_data = date.today()
            else:
                date_data_ddmmyyyy = date_data[6:] + "-" + date_data[3:5] + "-" + date_data[:2]
                date_data = date_data_ddmmyyyy

            beds_div = i.find('div', attrs={'class': 'rental-info'})
            beds = beds_div.find('span', attrs={'class': 'bedrooms'})
            beds = beds.text.replace('\n', '')
            beds = beds.replace(' ', '')
            beds_data = beds.strip()

            info_div = i.find('div', attrs={'class': 'description'})
            info = info_div.text.replace('\n', '')
            info_data = info.strip()

            price_div = i.find('div', attrs={'class': 'price'})
            price_data = price_div.text.strip()
            price_data = price_data.replace(',', '')
            curency_data = price_data[0]
            price_data = price_data[1:]

            conn = engine.connect()

            ins_data_query = parsed_data.insert().values(image=image_data, title_text=title_data, city=city_data, date_info=date_data, beds=beds_data, info=info_data, curency=curency_data, price=price_data)
            conn.execute(ins_data_query)


def parse_content():
    url = 'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273'
    get_content(url)


if __name__ == '__main__':
    parse_content()