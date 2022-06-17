import os
import requests
import pandas as pd
import re
import time

from FinanceAndMl_libs import finance_ml as fm
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
key_words = "dividend or per share or distribution"
sort_news = 'date'  # hits
keyword_type = 'boolean'  # boolean

base_url = 'https://www.stockwatch.com/'
headers = {
    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': "ru,en-US;q=0.9,en;q=0.8,ru-RU;q=0.7",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
}


def get_tiingo() -> pd.DataFrame:
    cur_disc = os.getcwd().split('\\')[0]
    df = pd.read_csv(f'{cur_disc}\Биржа\Stocks. BigData\Projects\APIs\API_Tiingo\supported_tickers 2022-06-03.csv')
    df = df[(df['priceCurrency'] == 'USD') & ~pd.isna(df['startDate']) & ~pd.isna(df['exchange']) &
            ~df['exchange'].isin(['LSE', 'SHE', 'ASX', 'SHG', 'NMFQS', 'CSE'])]

    return df


def get_asset_type(df_tiingo: pd.DataFrame, ticker: str) -> str:
    cur_df = df_tiingo[df_tiingo['ticker'] == ticker]
    if len(cur_df) == 0:
        print(ticker, cur_df)
        print('Нет тикера в Tiingo базе. Обработай вручную. Укажи Stock или иное.')
        return input()
    elif len(cur_df) == 1:
        return cur_df['assetType'].iloc[0]
    elif len(cur_df['assetType'].unique()) == 1:
        return cur_df['assetType'].unique()[0]
    else:
        print(ticker, cur_df)
        raise Exception('Несколько одинаковых тикеров в базе Tiingo')


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
        if re.search('dividend', headline, re.IGNORECASE) or re.search('Per Share', headline, re.IGNORECASE) or \
                re.search('distribution', headline, re.IGNORECASE):

            date, day_time = time_converter(data[0])
            ticker = data[1].get_text()
            link = 'https://www.stockwatch.com' + data[4].find('a')['href']
            dict_df['date'].append(date)
            dict_df['time'].append(day_time)
            dict_df['ticker'].append(ticker)
            dict_df['headline'].append(headline)
            dict_df['link'].append(link)

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
    final_df.to_csv(f'data\SpecDiv_{end_date}.csv', index=False)


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
        dict_df = {'date': [], 'time': [], 'ticker': [], 'headline': [], 'link': []}
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1) if end_date.weekday() != 0 else end_date - timedelta(days=3)
        news_finder(
            s, dict_df, key_words, sort_news, keyword_type, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
        )

        # Закрываем сессию
        s.close()


def filter_news():
    today = datetime.now()
    yesterday = today - timedelta(days=1) if today.weekday() != 0 else today - timedelta(days=3)
    df = pd.read_csv(f"data\SpecDiv_{today.strftime('%Y%m%d')}.csv", parse_dates=['date'])
    df_tiingo = get_tiingo()

    # Удалим неактуальные (если новость была вчера на открытии или вчера в течении дня)
    df = df[
        (df['date'].dt.strftime('%Y%m%d') == today.strftime('%Y%m%d')) |
        ((df['date'].dt.strftime('%Y%m%d') == yesterday.strftime('%Y%m%d')) & (df['time'].isin(['AMC', 'DAY'])))
    ]

    # Удалим ETF
    idx_drop = []
    for idx, row in df.iterrows():
        if get_asset_type(df_tiingo, row['ticker']) != 'Stock':
            idx_drop.append(idx)
    df.drop(idx_drop, axis=0, inplace=True)

    # Удалим не проходящие по объёму. Добавим данные.
    fm.download_tickers(df['ticker'], reload=False, threads=10)
    dict_data = fm.get_tickers(df['ticker'])
    idx_drop = []
    for idx, row in df.iterrows():
        ticker = row['ticker']
        if ticker not in dict_data.keys():
            continue
        cur_date = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
        cur_df = dict_data[ticker][:cur_date]

        prenews_price = cur_df['close'].iloc[-1]
        avg_vol = cur_df['volume'].iloc[-20:].mean()

        if (50_000 > avg_vol) or (avg_vol > 3_000_000):
            idx_drop.append(idx)
        else:
            df.loc[idx, 'AvgVol'] = avg_vol
            df.loc[idx, 'PrenewsPrice'] = prenews_price
    df.drop(idx_drop, axis=0, inplace=True)
    df['divAmount'] = None
    df['divToPrice'] = None
    df['exDivDate'] = None
    df['dayDiff'] = None

    # Если финальный файл уже существует, то добвим к тему недостающие данные
    path = f"data\SpecDiv_{today.strftime('%Y%m%d')}_final.csv"
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        new_df = pd.concat([old_df, df])
        new_df.drop_duplicates(subset=['ticker', 'link'], keep=False, inplace=True)
        new_df.to_csv(path, mode='a', header=False, index=False)
    else:
        df.drop_duplicates(subset=['ticker', 'link'], keep='first', inplace=True)
        df.to_csv(path, index=False)

    print(pd.read_csv(path))
    print("Рассчитай вручную, чтобы до дня ex-div date было менее 20 дней и чтобы див был более 2.5% от цены.")


if __name__ == "__main__":
    create_session(base_url, headers)
    filter_news()
    time.sleep(60)

    while datetime.now().hour < 10:
        hour = datetime.now().hour
        min = datetime.now().minute
        if (hour in [4, 5, 6, 7, 8, 9] and min == 1) or (hour == 9 and min in [15, 20]):
            print(f"Работаем. {datetime.now()}")
            create_session(base_url, headers)
            filter_news()
            time.sleep(60)

        time.sleep(60)

    # Прописать выставление ордеров.
