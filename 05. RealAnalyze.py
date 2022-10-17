import datetime
import pandas as pd
import os

PATH = os.path.abspath(os.getcwd())


# Settings
desired_width = 1_800
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 1_000)
pd.set_option('display.max_columns', 100)


def all_real_news_to_one_file():
    cur_path = os.path.join(PATH, r'dividends_data\real')
    final_df = pd.DataFrame()
    for file in os.listdir(cur_path):
        if file.endswith('csv') and 'final' in file:
            cur_df = pd.read_csv(os.path.join(cur_path, file))
            final_df = pd.concat([final_df, cur_df])

    final_df.drop_duplicates(keep='first', inplace=True)
    cur_date = datetime.datetime.now().strftime('%Y-%m-%d')
    final_df.to_csv(os.path.join(cur_path, f'final_df_{cur_date}.csv'), index=False)

    return None


def one_file_like_backtest():
    cur_path = PATH.replace('MainResearch', r'dividends_data\real')
    cur_df = pd.read_csv(os.path.join(cur_path, 'final_df_2022-08-05.csv'), encoding='latin-1')

    cur_df = cur_df[~pd.isna(cur_df['exDivDate'])].copy()
    cur_df.rename(columns={
        'exDivDate': 'Ex-dividend date', 'divAmount': 'divCash', 'divToPrice': 'div_ratio', 'date': 'DecDay',
        'time': 'Time', 'link': 'Link', 'PrenewsPrice': 'PreNewsPrice'
    }, inplace=True)
    cur_df['AssetType'] = 'Stock'

    cur_df.to_csv(os.path.join(cur_path, 'final_df_backtest_2022-08-05.csv'), index=False)


if __name__ == "__main__":
    # all_real_news_to_one_file()
    one_file_like_backtest()