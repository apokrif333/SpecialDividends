from datetime import datetime, timedelta
from APIs.API_Brokers.IB import IbAdvisor
from ib_insync import *
from pprint import pprint

import pandas_market_calendars as mcal
import pandas as pd
import os

cur_disc = os.getcwd().split('\\')[0]


def get_df() -> pd.DataFrame:
    date = datetime.now().strftime('%Y%m%d')
    path = f"{cur_disc}\Биржа\Stocks. BigData\Projects\Researchers\SpecialDividends\MainResearch\data\SpecDiv_{date}_final.csv"
    df = pd.read_csv(path)
    df = df[(df['divToPrice'] > 0.025) & (df['dayDiff'] < 20) & (df['assetType'] == 'Stock')]
    print(df)

    return df


def connect_ib(df: pd.DataFrame):
    port = 7498
    ib_class = IbAdvisor(port, '', {}, {}, {}, price_downloader='TOS')
    ib_class.connect()
    ib_class.get_all_orders_positions()
    ib_class.get_acc_values('NetLiquidationByCurrency')
    ib_class.get_prices(ib_class.generate_contracts(df['ticker'].to_list()))

    return ib_class


def calc_prev_day(df: pd.DataFrame,  ticker: str) -> str:
    end = df[df['ticker'] == ticker]['recordDate'].iloc[0]
    start = pd.to_datetime(end) - timedelta(days=8)

    nyse = mcal.get_calendar('NYSE')
    pre_div_date = nyse.schedule(start_date=start, end_date=end).index[-3]
    pre_div_date = (pre_div_date + timedelta(hours=10)).strftime('%Y%m%d %H:%M:%S')

    return pre_div_date


def print_logs(ib_class, trade) -> None:
    while not trade.isActive():
        ib_class.ib.waitOnUpdate()
    print('-------------')
    print(trade.order.orderId, trade.contract.symbol, trade.order.action, trade.order.orderType, trade.order.auxPrice)
    pprint(trade.log)

    return None


def run_special_div(account: str = 'U1717377', max_positions: int = 10, stop_loss: float = 0.1,
                    action: str = 'position') -> None:
    """
    Выставить сделки, согласно сгенерированному файлу из скрипта 04. NewsFinder.py

    @param account: номер счёта в IB
    @param max_positions: делитель для расчёта капитала в одной позиции
    @param stop_loss: процент отступа от цены входа, для выставления стопа
    @param action: position (выставить MOO на покупку), orders (выставить стопы и MOC),
        cancel_mocs (отменить MOC, если позицию выбило по стопу)
    @return: None

    """
    df = get_df()
    ib_class = connect_ib(df)
    acc_data = ib_class.accounts_data[account]

    # Выставим MOO
    if action == 'position':
        sum_per_position = acc_data['NetLiquidationByCurrency'] / max_positions
        for ticker in df['ticker']:
            skeep_cur_ticker = False
            if len(acc_data['orders']) != 0:
                for order in acc_data['orders']:
                    if (order.symbol == ticker) & (order.tif == 'OPG'):
                        skeep_cur_ticker = True
                        break
            if skeep_cur_ticker:
                continue

            cur_price = ib_class.symbols_prices[ticker]['last']
            size = int(sum_per_position / cur_price)
            order = Order(**{
                'account': account, 'action': 'BUY', 'tif': 'OPG', 'orderType': 'MKT', 'totalQuantity': size,
                'transmit': True
            })
            trade = ib_class.ib.placeOrder(ib_class.generate_contracts([ticker])[0], order)
            print_logs(ib_class, trade)

    # Выставим стопы и профиты
    elif (action == 'orders') & (len(acc_data['positions']) != 0):
        for pos in acc_data['positions']:
            ticker = pos.contract.symbol
            if ticker not in df['ticker'].values:
                continue

            make_stop = True
            make_moc = True
            if len(acc_data['orders']) != 0:
                for order in acc_data['orders']:
                    if order.symbol == ticker:
                        if order.orderType == 'STP':
                            make_stop = False
                        if order.orderType == 'MOC':
                            make_moc = False

            side = 'SELL' if pos.position > 0 else 'BUY'
            price = pos.avgCost * (1 - stop_loss) if side == 'SELL' else pos.avgCost * (1 + stop_loss)
            if make_moc:
                order = Order(**{
                    'account': account, 'action': side, 'tif': 'DAY', 'orderType': 'MOC', 'totalQuantity': pos.position,
                    'transmit': True, 'goodAfterTime': calc_prev_day(df, ticker),
                })
                trade = ib_class.ib.placeOrder(ib_class.generate_contracts([ticker])[0], order)
                print_logs(ib_class, trade)
            if make_stop:
                order = Order(**{
                    'account': account, 'action': side, 'tif': 'GTC', 'orderType': 'STP', 'totalQuantity': pos.position,
                    'lmtPrice': 0.0, 'auxPrice': round(price, 2), 'transmit': True
                })
                trade = ib_class.ib.placeOrder(ib_class.generate_contracts([ticker])[0], order)
                print_logs(ib_class, trade)

    elif (action == 'cancel_mocs') and (len(acc_data['orders']) != 0):
        cur_tickers = []
        for pos in acc_data['positions']:
            cur_tickers.append(pos.contract.symbol)

        for order in acc_data['orders']:
            if order.orderType != 'MOC':
                continue
            if order.symbol not in cur_tickers:
                trade = ib_class.ib.cancelOrder(order)
                print(f"Отменяем {order.symbol} {order.orderType}")
                # print_logs(ib_class, trade)
    else:
        print(f"Неизвестный тип action - {action}")

    return None


if __name__ == "__main__":
    run_special_div(action='orders')
