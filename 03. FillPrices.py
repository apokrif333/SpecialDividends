import pandas as pd
import os
import dateutil

from datetime import timedelta
from FinanceAndMl_libs import finance_ml as fm

# Settings
pd.set_option('max_rows', 1_000)
pd.set_option('max_columns', 100)
pd.set_option('display.width', 1_800)


def delete_error_date(df: pd.DataFrame) -> pd.DataFrame:
    index_for_del = []
    for idx, row in df.iterrows():
        try:
            pd.to_datetime(row['Ex-dividend date']).strftime('%Y-%m-%d')
        except dateutil.parser._parser.ParserError:
            index_for_del.append(idx)

    return df.drop(index_for_del, axis=0)


def get_tiingo() -> pd.DataFrame:
    cur_disc = os.getcwd().split('\\')[0]
    df = pd.read_csv(f'{cur_disc}\Биржа\Stocks. BigData\Projects\APIs\API_Tiingo\supported_tickers 2022-06-03.csv')
    df = df[(df['priceCurrency'] == 'USD') & ~pd.isna(df['startDate']) & ~pd.isna(df['exchange']) &
            ~df['exchange'].isin(['LSE', 'SHE', 'ASX', 'SHG', 'NMFQS', 'CSE'])]

    return df


def get_asset_type(df_tiingo: pd.DataFrame, ticker: str) -> str:
    cur_df = df_tiingo[df_tiingo['ticker'] == ticker]
    if len(cur_df) == 0:
        return 'unknown'
    elif len(cur_df) == 1 or (ticker in ['RTL']):
        return cur_df['assetType'].iloc[0]
    elif len(cur_df['assetType'].unique()) == 1:
        return cur_df['assetType'].unique()[0]
    else:
        print(ticker, cur_df)
        raise Exception('Несколько одинаковых тикеров в базе Tiingo')


if __name__ == "__main__":
    df_tiingo = get_tiingo()

    set_date = '2022-01-01'
    df_main = pd.read_csv(f"SpecDiv_onlyTiingo_{set_date}.csv")
    df_main = df_main[df_main['Time'] != '-']
    df_main = df_main[
        ['date', 'divCash', 'splitFactor', 'div_ratio', 'ticker', 'DecDay', 'Time', 'Link', 'FundamentLink', 'headline']
    ]
    df_main = df_main.rename(columns={'date': 'Ex-dividend date'})
    # df_main = delete_error_date(df_main)

    df_main['Ex-dividend date'] = pd.to_datetime(df_main['Ex-dividend date'])
    df_main['DecDay'] = pd.to_datetime(df_main['DecDay'])
    df_main = df_main[df_main['Time'] != 'Error'].reset_index(drop=True)

    data_path = r"E:\Биржа\Stocks. BigData\Цены\Дейли\tiingo\usa"
    # fm.download_tickers(df_main['ticker'].unique())
    dict_data = fm.get_tickers(df_main['ticker'].unique())
    load_tickers = list(dict_data.keys())

    for idx, row in df_main.iterrows():
        print(idx, row['ticker'])

        if row['ticker'] not in load_tickers:
            continue

        cur_date = pd.to_datetime(row['DecDay']).strftime('%Y-%m-%d')
        exdiv_date = pd.to_datetime(row['Ex-dividend date']).strftime('%Y-%m-%d')
        dict_data[row['ticker']].columns = list(map(lambda x: x.lower(), dict_data[row['ticker']].columns))
        dict_data[row['ticker']] = dict_data[row['ticker']].rename(columns={
            'adjopennodiv': 'open', 'adjhighnodiv': 'high', 'adjlownodiv': 'low', 'adjclosenodiv': 'close'
        })
        cur_df = dict_data[row['ticker']][cur_date:exdiv_date]

        if len(cur_df) < 3 or cur_date not in cur_df.index:
            continue

        if row['Time'] == 'AMC':
            df_main.loc[idx, 'PreNewsPrice'] = cur_df['close'].iloc[0]
            df_main.loc[idx, 'OpenPrice'] = cur_df['open'].iloc[1]
            df_main.loc[idx, 'MaxBeforeRecord'] = cur_df['high'].iloc[1:-1].max()
            df_main.loc[idx, 'MinBeforeRecord'] = cur_df['low'].iloc[1:-1].min()
            df_main.loc[idx, 'CloseBeforeRecord'] = cur_df['close'].iloc[-2]
            df_main.loc[idx, 'OpenAtRecord'] = cur_df['open'].iloc[-1]
            df_main.loc[idx, 'AvgVol'] = dict_data[row['ticker']][:cur_date]['volume'].iloc[-20:].mean()
        elif row['Time'] == 'BMO':
            df_main.loc[idx, 'PreNewsPrice'] = dict_data[row['ticker']][:cur_date]['close'].iloc[-2]
            df_main.loc[idx, 'OpenPrice'] = cur_df['open'].iloc[0]
            df_main.loc[idx, 'MaxBeforeRecord'] = cur_df['high'].iloc[:-1].max()
            df_main.loc[idx, 'MinBeforeRecord'] = cur_df['low'].iloc[:-1].min()
            df_main.loc[idx, 'CloseBeforeRecord'] = cur_df['close'].iloc[-2]
            df_main.loc[idx, 'OpenAtRecord'] = cur_df['open'].iloc[-1]
            df_main.loc[idx, 'AvgVol'] = dict_data[row['ticker']][:cur_date]['volume'].iloc[-21:-1].mean()
        elif row['Time'] == 'DAY':
            df_main.loc[idx, 'PreNewsPrice'] = cur_df['open'].iloc[0]
            df_main.loc[idx, 'OpenPrice'] = cur_df['close'].iloc[0]
            df_main.loc[idx, 'MaxBeforeRecord'] = cur_df['high'].iloc[1:-1].max()
            df_main.loc[idx, 'MinBeforeRecord'] = cur_df['low'].iloc[1:-1].min()
            df_main.loc[idx, 'CloseBeforeRecord'] = cur_df['close'].iloc[-2]
            df_main.loc[idx, 'OpenAtRecord'] = cur_df['open'].iloc[-1]
            df_main.loc[idx, 'AvgVol'] = dict_data[row['ticker']][:cur_date]['volume'].iloc[-21:-1].mean()
        else:
            raise Exception(f"Неверный тип Time - {row['Time']}")

        df_main.loc[idx, 'AssetType'] = get_asset_type(df_tiingo, row['ticker'])

    df_main['Ex-dividend date'] = df_main['Ex-dividend date'].dt.strftime('%Y-%m-%d')
    df_main.to_csv(f'SpecDiv_onlyTiingo_prices_{set_date}.csv', index=False)
