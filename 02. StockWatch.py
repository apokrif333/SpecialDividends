import os

import requests
import pandas as pd
import re
import time
import json
import lxml
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Settings
pd.set_option('max_rows', 1_000)
pd.set_option('max_columns', 100)
pd.set_option('display.width', 1_200)


cur_disc = os.getcwd().split('\\')[0]
user_name = 'kvvtamfarsqyuphhyf@bvhrk.com'
password = '659434'
key_words = '"Special Dividend" +declare'
start_date = '19980101'
end_date = '19991231'
sort_news = 'date'  # hits
keyword_type = 'nat'  # boolean

base_url = 'https://www.stockwatch.com/'
headers = {
    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': "ru,en-US;q=0.9,en;q=0.8,ru-RU;q=0.7",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
}


def make_data_for_request(soup: BeautifulSoup, names_for_pop: list, data_change: dict) -> dict:
    data = {i['name']: i.get('value', '') for i in soup.select('input[name]')}

    for key in names_for_pop:
        data.pop(key)
    for key, value in data_change.items():
        data[key] = value

    return data


def time_converter(raw_data) -> (datetime, str):
    date = pd.to_datetime(raw_data.get_text()) + timedelta(hours=3)
    news_time = datetime.strptime(f"{date.hour}-{date.minute}", '%H-%M')
    if news_time > datetime.strptime(f"15-50", '%H-%M'):
        day_time = 'AMC'
    elif news_time < datetime.strptime(f"9-30", '%H-%M'):
        day_time = 'BMO'
    else:
        day_time = 'DAY'

    return date, day_time


def fill_news_file(soup: BeautifulSoup, dict_df: dict) -> dict:
    news_table = soup.find('table', border=1).find_all("tr")
    for row in news_table:
        data = row.find_all('td')
        if len(data) == 0 or 'Page' in data[0].get_text():
            continue

        headline = data[4].get_text()
        if re.search('special', headline, re.IGNORECASE) is None or \
                re.search('dividend', headline, re.IGNORECASE) is None:
            continue

        date, day_time = time_converter(data[0])
        ticker = data[1].get_text()
        link = 'https://www.stockwatch.com' + data[4].find('a')['href']
        fundament_link = 'https://www.stockwatch.com/Quote/Fundamentals?U:' + ticker

        dict_df['Date'].append(date)
        dict_df['Time'].append(day_time)
        dict_df['Ticker'].append(ticker)
        dict_df['Link'].append(link)
        dict_df['Fundament'].append(fundament_link)
        dict_df['RecordDate'].append(None)
        dict_df['SpecDiv Amount'].append(None)
        dict_df['OpenPrice'].append(None)
        dict_df['MaxBeforeRecord'].append(None)
        dict_df['MinBeforeRecord'].append(None)
        dict_df['CloseBeforeRecord'].append(None)
        dict_df['OpenAtRecord'].append(None)

    return dict_df


def news_finder(s: requests.Session, dict_df: dict, key_words: str, sort_news: str, keyword_type: str, start_date: str,
                end_date: str):
    # Получение новостей ------------------------------------------------------------------------------------------
    r = s.get('https://www.stockwatch.com/News/Search.aspx')
    soup = BeautifulSoup(r.text, "lxml")
    names_for_pop = [
        'ctl00$CheckChart2', 'ctl00$CheckCloses2', 'ctl00$CheckDepth2', 'ctl00$GoButton2',
        'ctl00$MainContent$bDate', 'ctl00$MainContent$bKeyword', 'ctl00$MainContent$bSymbol',
        'ctl00$MainContent$bToday', 'ctl00$MainContent$bType'
    ]
    data_change = {
        'ctl00$RadioRegion2': 'RadioUS2', 'ctl00$CheckQuote2': 'on', 'ctl00$CheckNews2': 'on',
        'ctl00$MainContent$cSymbol': 'on', 'ctl00$MainContent$dTodayRegion': 'С',
        'ctl00$MainContent$dSymbolFeed': 'C', 'ctl00$MainContent$dType': '200',
        'ctl00$MainContent$tKeywords': key_words, 'ctl00$MainContent$dKeywordFeed': 'usbull',
        'ctl00$MainContent$dKeywordSort': sort_news, 'ctl00$MainContent$dKeywordStemming': 'Y',
        'ctl00$MainContent$dKeywordType': keyword_type, 'ctl00$MainContent$dKeywordFuzzy': '0',
        'ctl00$MainContent$dKeywordPhonic': 'N', 'ctl00$MainContent$bKeyword.x': '41',
        'ctl00$MainContent$bKeyword.y': '10', 'ctl00$MainContent$dEx': '',
        'ctl00$MainContent$dDateSort': 'timedesc', 'ctl00$MainContent$dDateFeed': 'C',
        'ctl00$MainContent$tKeywordFrom': start_date, 'ctl00$MainContent$tKeywordTo': end_date
    }
    data = make_data_for_request(soup, names_for_pop, data_change)

    r = s.post('https://www.stockwatch.com/News/Search.aspx', data=data)
    soup = BeautifulSoup(r.text, "lxml")

    # Заполенение таблицы с новостями ------------------------------------------------------------------------------
    dict_df = fill_news_file(soup, dict_df)
    print('Первая страница получена успешно')

    # Итератор по страницам
    td = [i for i in soup.select('td[colspan]')]
    if len(td) > 0:
        td = td[0]
        pages_dict = {}
        for a in td.findAll('a'):
            pages_dict[a.text] = re.findall("\'(.*?)\'", a['href'])[0]
        last_page = a.text

        for page, link in pages_dict.items():
            print(f'Получаем {page} страницу из {last_page}')

            names_for_pop = [
                'ctl00$CheckChart2', 'ctl00$CheckCloses2', 'ctl00$CheckDepth2', 'ctl00$GoButton2',
                'ctl00$ImageButton1'
            ]
            data_change = {
                '__EVENTTARGET': link, 'ctl00$CheckNews2': 'on', 'ctl00$CheckQuote2': 'on',
                'ctl00$RadioRegion2': 'RadioUS2'
            }
            data = make_data_for_request(soup, names_for_pop, data_change)
            r = s.post('https://www.stockwatch.com/News/Search.aspx', data=data)
            soup = BeautifulSoup(r.text, "lxml")
            dict_df = fill_news_file(soup, dict_df)

            print(f'{page} страница получена успешно')

    # Сохраним
    final_df = pd.DataFrame.from_dict(dict_df)
    final_df.to_csv('SpecDiv.csv', mode='a', header=False, index=False)


def find_dividend_size(s: requests.Session):
    save_iter = 0
    df = pd.read_csv('SpecDiv.csv')
    for idx, df_row in df.iterrows():
        print(f"{idx} строка из {len(df)-1}")
        if pd.isna(df_row['Ex-DivDate']) is False:
            continue

        news_date = pd.to_datetime(df_row['Date'])
        r = s.get(df_row['Fundament'])
        soup = BeautifulSoup(r.text, "lxml")

        div_table = soup.find('table', id='MainContent_DividendList_gDividends').find_all('tr')
        if len(div_table) == 1:
            continue

        div_dict = {}
        for row in div_table:
            if row['class'][0] == 'gridViewHeaderRow':
                for col in row.find_all('th'):
                    div_dict[col.get_text()] = []
            else:
                cols_names = list(div_dict.keys())
                values = row.find_all('td')
                for i in range(len(cols_names)):
                    div_dict[cols_names[i]].append(values[i].get_text())

        div_df = pd.DataFrame.from_dict(div_dict)
        div_df['Ex-Div Date'] = pd.to_datetime(div_df['Ex-Div Date'])
        div_df = div_df[div_df['Ex-Div Date'] > news_date]
        if len(div_df) == 0:
            continue

        div_df = div_df.iloc[-1]
        df.loc[idx, 'Ex-DivDate'] = div_df['Ex-Div Date']
        df.loc[idx, 'SpecDiv Amount'] = div_df['Amount']
        df.loc[idx, 'PayDate'] = div_df['Payable Date']

        time.sleep(2)
        save_iter += 1
        if save_iter == 10:
            save_iter = 0
            df.to_csv('SpecDiv.csv', index=False)

    df.to_csv('SpecDiv.csv', index=False)


def news_for_exist_exdiv_date(s: requests.Session, set_date: str):
    df_main = pd.read_csv(f'SpecDiv_onlyTiingo_{set_date}.csv', parse_dates=['date'])
    if 'DecDay' not in df_main.columns:
        df_main['DecDay'] = '-'
    df_main = df_main[df_main['volume'] > 25_000].reset_index(drop=True)

    count = 0
    for idx, row in df_main.iterrows():
        print(f"{idx} - {len(df_main)}")
        if row['DecDay'] != '-':
            continue

        ticker = row['ticker']
        end_date = row['date'].strftime('%Y%m%d')
        start_date = (row['date'] - timedelta(days=60)).strftime('%Y%m%d')
        print(ticker, start_date, end_date)

        # Получение новостей ------------------------------------------------------------------------------------------
        r = s.get('https://www.stockwatch.com/News/Search.aspx')
        soup = BeautifulSoup(r.text, "lxml")
        names_for_pop = [
            'ctl00$CheckChart2', 'ctl00$CheckCloses2', 'ctl00$CheckDepth2', 'ctl00$GoButton2',
            'ctl00$MainContent$bDate', 'ctl00$MainContent$bKeyword', 'ctl00$MainContent$bSymbol',
            'ctl00$MainContent$bToday', 'ctl00$MainContent$bType', 'ctl00$ImageButton1',
        ]
        data_change = {
            'ctl00$RadioRegion2': 'RadioUS2', 'ctl00$CheckQuote2': 'on', 'ctl00$CheckNews2': 'on',
            'ctl00$MainContent$cSymbol': 'on', 'ctl00$MainContent$dTodayRegion': 'С',
            'ctl00$MainContent$dSymbolFeed': 'U', 'ctl00$MainContent$dType': '200',
            'ctl00$MainContent$tKeywords': '', 'ctl00$MainContent$dKeywordFeed': 'swbull',
            'ctl00$MainContent$dKeywordSort': 'hits', 'ctl00$MainContent$dKeywordStemming': 'Y',
            'ctl00$MainContent$dKeywordType': 'nat', 'ctl00$MainContent$dKeywordFuzzy': '0',
            'ctl00$MainContent$dKeywordPhonic': 'N', 'ctl00$MainContent$dEx': '',
            'ctl00$MainContent$dDateSort': 'timedesc', 'ctl00$MainContent$dDateFeed': 'C',
            'ctl00$MainContent$tSymbol': ticker, 'ctl00$MainContent$tSymbolFrom': start_date,
            'ctl00$MainContent$tSymbolTo': end_date, 'ctl00$MainContent$bSymbol.x': '41',
            'ctl00$MainContent$bSymbol.y': '4',
        }
        data = make_data_for_request(soup, names_for_pop, data_change)

        r = s.post('https://www.stockwatch.com/News/Search.aspx', data=data)
        soup = BeautifulSoup(r.text, "lxml")

        # Поиск конкретной новости -------------------------------------------------------------------------------------
        news_table = soup.find('table', border=1).find_all("tr")
        date, day_time, link, fundament_link, head = '-', '-', '-', '-', '-'
        for table_row in news_table:
            data = table_row.find_all('td')

            if len(data) == 0 or 'bulletins' in data[0].get_text() or 'Page' in data[0].get_text():
                continue

            headline = data[4].get_text()
            if re.search('dividend', headline, re.IGNORECASE) or re.search('Per Share', headline, re.IGNORECASE) or \
                    re.search('distribution', headline, re.IGNORECASE) or re.search('spin-off', headline, re.IGNORECASE):
                date, day_time = time_converter(data[0])
                link = 'https://www.stockwatch.com' + data[4].find('a')['href']
                fundament_link = 'https://www.stockwatch.com/Quote/Fundamentals?U:' + ticker
                head = headline
                break

        df_main.loc[idx, 'DecDay'] = date
        df_main.loc[idx, 'Time'] = day_time
        df_main.loc[idx, 'Link'] = link
        df_main.loc[idx, 'FundamentLink'] = fundament_link
        df_main.loc[idx, 'headline'] = head

        count += 1
        if count == 5:
            df_main.to_csv(f'SpecDiv_onlyTiingo_{set_date}.csv', index=False)
            count = 0

        # time.sleep(1)

    df_main.to_csv(f'SpecDiv_onlyTiingo_{set_date}.csv', index=False)


def get_headers_by_link(s: requests.Session) -> None:
    df_main = pd.read_csv(f"SpecDiv_onlyTiingo_pricesHeads.csv")
    if 'headline' not in df_main.columns:
        df_main['headline'] = '-'

    counter = 0
    for idx, row in df_main.iterrows():
        print(idx / len(df_main))
        if (df_main['headline'][idx] != '-') or (df_main['AvgVol'][idx] < 25_000):
            continue

        link = row['Link']
        r = s.get(link)
        soup = BeautifulSoup(r.text, "lxml")
        headline = soup.find('span', {'id': 'MainContent_NewsSubject'})
        if headline is None:
            df_main.loc[idx, 'headline'] = 'error'
        else:
            df_main.loc[idx, 'headline'] = headline.text

        if counter >= 20:
            df_main.to_csv("SpecDiv_onlyTiingo_pricesHeads.csv", index=False)
            counter = 0
        counter += 1

        time.sleep(1)

    return None


def create_session(base_url: str, headers: dict, cur_disc: str = cur_disc):
    with requests.Session() as s:
        # Логин --------------------------------------------------------------------------------------------------------
        s.headers['User-Agent'] = headers['User-Agent']
        r = s.get(base_url)
        soup = BeautifulSoup(r.text, "lxml")

        # Куки для логина
        AntiXsrfToken = r.headers['Set-Cookie'].split(';')[0]
        pref = r.headers['Set-Cookie'].split(';')[2].split(',')[1]  # .replace("%7cN%7cN%7", "%7cN%7cY%7")
        s.headers = headers
        s.headers['Content-Length'] = '5107'  #  r.headers['Content-Length']
        s.headers['Referer'] = base_url
        s.headers['Cookie'] = ';'.join([AntiXsrfToken, pref])

        # Data для логина
        names_for_pop = [
            'ctl00$CheckChart2', 'ctl00$CheckCloses2', 'ctl00$CheckDepth2', 'ctl00$GoButton2',
            'ctl00$MainContent$HpIndexesChart4$bIndexRemove', 'ctl00$MainContent$HpIndexesChart5$bIndexRemove'
        ]
        data_change = {
            'ctl00$RadioRegion2': 'RadioUS2', 'ctl00$CheckQuote2': 'on', 'ctl00$CheckNews2': 'on',
            'ctl00$PowerUserName': user_name, 'ctl00$PowerRememberMe': 'on', 'ctl00$PowerPassword': password,
            'ctl00$Login.x': '40', 'ctl00$Login.y': '9', 'ctl00$MainContent$cActive$ListEx': 'T',
            'ctl00$MainContent$cActive$RadioTop10': 'RadioGain'
        }
        data = make_data_for_request(soup, names_for_pop, data_change)

        # Получение ключа
        r = s.post(base_url, data=data, allow_redirects=False)
        key_xxx = r.headers['Set-Cookie'].split(';')[0]
        headers = {'User-Agent': headers['User-Agent'], 'Cookie': '; '.join([s.headers['Cookie'], key_xxx])}
        s.headers = headers
        print("Логин-ключ получен")
        time.sleep(3)

        # Получение новостей по определённому слову
        # col_for_data = pd.read_csv('SpecDiv.csv').columns
        # dict_df = {col: [] for col in col_for_data}
        # news_finder(s, dict_df, key_words, sort_news, keyword_type, start_date, end_date)

        # Получение дивов из базы StockWatch
        # find_dividend_size(s)

        # Поиск новостей по ключ-словам, в определённом диапазоне времени
        set_date = '2022-04-01'
        news_for_exist_exdiv_date(s, set_date)

        # Сбор заголовков новостей по известной ссылке
        # get_headers_by_link(s)

        # Закрываем сессию
        s.close()


if __name__ == "__main__":
    create_session(base_url, headers)
