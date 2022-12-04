import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from flask import Flask, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from linebot import (LineBotApi, WebhookHandler)
from linebot.exceptions import (InvalidSignatureError)
from linebot.models import (MessageEvent, TextMessage, TextSendMessage)
import twstock
import twder

app = Flask(__name__)

headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'
}

gsp_scopes = ['https://spreadsheets.google.com/feeds']

SPREAD_SHEETS_KEY = os.environ.get('SPREAD_SHEETS_KEY')

year = os.environ.get('year')
month = os.environ.get('month')
stock_no = os.environ.get('stock_no')
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET')
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)


def get_google_sheets_creds_dict():
    google_sheets_creds = {
        'type': os.environ.get('GOOGLE_SHEETS_TYPE'),
        'project_id': os.environ.get('GOOGLE_SHEETS_PROJECT_ID'),
        'private_key_id': os.environ.get('GOOGLE_SHEETS_PRIVATE_KEY_ID'),
        'private_key': os.environ.get('GOOGLE_SHEETS_PRIVATE_KEY'),
        'client_email': os.environ.get('GOOGLE_SHEETS_CLIENT_EMAIL'),
        'client_id': os.environ.get('GOOGLE_SHEETS_CLIENT_ID'),
        'auth_uri': os.environ.get('GOOGLE_SHEETS_AUTH_URI'),
        'token_uri': os.environ.get('GOOGLE_SHEETS_TOKEN_URI'),
        'auth_provider_x509_cert_url': os.environ.get('GOOGLE_SHEETS_AUTH_PROVIDER_X509_CERT_URL'),
        'client_x509_cert_url': os.environ.get('GOOGLE_SHEETS_CLIENT_X509_CERT_URL')
    }
    return google_sheets_creds

google_sheets_creds_dict = get_google_sheets_creds_dict()

def auth_gsp_client(creds_dict, scopes):
    creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    return gspread.authorize(credentials)

gsp_client = auth_gsp_client(google_sheets_creds_dict, gsp_scopes)


worksheet = gsp_client.open_by_key(SPREAD_SHEETS_KEY).worksheet('stock_price')

def crawl_for_stock_price(stock_no):
    print('擷取股票代號:', stock_no)
    url = f'https://goodinfo.tw/StockInfo/ShowK_ChartFlow.asp?RPT_CAT=PER&STOCK_ID={stock_no}&CHT_CAT=YEAR'

    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = 'utf-8'

    raw_html = resp.text

    soup = BeautifulSoup(raw_html, 'html.parser')
    per_rows = []
    eps_rows = []
    for row_line in range(0, 5):
        eps_rows.append(soup.select(f'#row{row_line} td')[4].text)
        per_rows.append(soup.select(f'#row{row_line} td')[5].text)


    max_eps = float(max(eps_rows))
    min_eps = float(min(eps_rows))

    max_per = float(max(per_rows))
    min_per = float(min(per_rows))

    high_price = max_eps * max_per
    low_price = min_eps * min_per
    middle_price = (high_price + low_price) / 2
    print('開始寫入資料...')
    worksheet.insert_row([stock_no, high_price, middle_price, low_price], 2)
    print('成功寫入資料...')

def crawl_for_stock_price_job():
    worksheet.clear()
    print('開始寫入標頭...')
    worksheet.insert_row(['stock_no', 'high_price', 'middle_price', 'low_price'], 1)
    print('成功寫入標頭...')
    stock_no_list = ['2330']
    crawl_for_stock_price(stock_no_list[0])

def get_check_price_rule_message(stock_no, high_price, middle_price, low_price, latest_trade_price):
    if latest_trade_price > high_price:
        message_str = f'台積電({stock_no}):目前股價太貴了!(成交價:{latest_trade_price}>昂貴價:{high_price})'
    elif high_price > latest_trade_price and latest_trade_price > middle_price:
        message_str = f'台積電({stock_no}):目前股價介於昂貴價和合理價之間!(昂貴價:{high_price}>成交價:{latest_trade_price}>合理價:{middle_price})'
    elif middle_price > latest_trade_price and latest_trade_price > low_price:
        message_str = f'台積電({stock_no}):目前股價介於合理價和便宜價之間!(合理價:{middle_price}>成交價:{latest_trade_price}>便宜價:{low_price})'
    elif low_price > latest_trade_price:
        message_str = f'台積電({stock_no}):目前股價很便宜!(便宜價:{low_price}>成交價:{latest_trade_price})'
    return message_str


fund_map_dict = {}

def init_fund_list():
    resp = requests.get(f'https://www.sitca.org.tw/ROC/Industry/IN2421.aspx?txtMonth={month}&txtYear={year}', headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    table_content = soup.select('#ctl00_ContentPlaceHolder1_TableClassList')[0]
    fund_links = table_content.select('a')

    for fund_link in fund_links:
        if fund_link.text:
            fund_name = fund_link.text
            fund_group_id = fund_link['href'].split('txtGROUPID=')[1]
            fund_map_dict[fund_name] = fund_group_id


def fetch_fund_rule_items(year, month, group_id):

    fetch_url = f'https://www.sitca.org.tw/ROC/Industry/IN2422.aspx?txtYEAR={year}&txtMONTH={month}&txtGROUPID={group_id}'
    print(year, month, group_id, fetch_url)
    resp = requests.get(fetch_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')

    table_content = soup.select('#ctl00_ContentPlaceHolder1_TableClassList')[0]

    fund_df = pd.read_html(table_content.prettify(), encoding='utf-8')[1]

    fund_df = fund_df.drop(index=[0])
    fund_df.columns = fund_df.iloc[0]
    fund_df = fund_df.drop(index=[1])
    fund_df.reset_index(drop=True, inplace=True)
    fund_df = fund_df.fillna(value=0)

    fund_df['一個月'] = fund_df['一個月'].astype(float)
    fund_df['三個月'] = fund_df['三個月'].astype(float)
    fund_df['六個月'] = fund_df['六個月'].astype(float)
    fund_df['一年'] = fund_df['一年'].astype(float)
    fund_df['二年'] = fund_df['二年'].astype(float)
    fund_df['三年'] = fund_df['三年'].astype(float)
    fund_df['五年'] = fund_df['五年'].astype(float)
    fund_df['自今年以來'] = fund_df['自今年以來'].astype(float)

    quarter_of_row_count = len(fund_df.index) // 4
    one_third_of_row_count = len(fund_df.index) // 3

    rule_4_one_year_df = fund_df.sort_values(by=['一年'], ascending=['True']).nlargest(quarter_of_row_count, '一年')

    rule_4_two_year_df = fund_df.sort_values(by=['二年'], ascending=['True']).nlargest(quarter_of_row_count, '二年')
    rule_4_three_year_df = fund_df.sort_values(by=['三年'], ascending=['True']).nlargest(quarter_of_row_count, '三年')
    rule_4_five_year_df = fund_df.sort_values(by=['五年'], ascending=['True']).nlargest(quarter_of_row_count, '五年')
    rule_4_this_year_df = fund_df.sort_values(by=['自今年以來'], ascending=['True']).nlargest(quarter_of_row_count, '自今年以來')

    rule_3_six_month_df = fund_df.sort_values(by=['六個月'], ascending=['True']).nlargest(one_third_of_row_count, '六個月')

    rule_3_three_month_df = fund_df.sort_values(by=['三個月'], ascending=['True']).nlargest(one_third_of_row_count, '三個月')

    rule_4_23_df = pd.merge(rule_4_two_year_df, rule_4_three_year_df, how='inner')
    rule_4_235_df = pd.merge(rule_4_23_df, rule_4_five_year_df, how='inner')
    rule_4_2350_df = pd.merge(rule_4_235_df, rule_4_this_year_df, how='inner')

    rule_first_4_df = rule_4_one_year_df
    rule_second_4_df = rule_4_2350_df
    rule_third_3_df = rule_3_six_month_df
    rule_forth_3_df = rule_3_three_month_df

    rule_44_df = pd.merge(rule_first_4_df, rule_second_4_df, how='inner')
    rule_443_df = pd.merge(rule_44_df, rule_third_3_df, how='inner')
    rule_4433_df = pd.merge(rule_443_df, rule_forth_3_df, how='inner')

    fund_rule_items_str = ''

    for index, row in rule_4433_df.iterrows():
        fund_rule_items_str += f'編號:{index+1},\n基金名稱:{row["基金名稱"]},\n一年報酬率:{row["一年"]},\n二年報酬率:{row["二年"]},\n三年報酬率:{row["三年"]},\n五年報酬率:{row["五年"]},\n自今年以來報酬率:{row["自今年以來"]},\n六個月報酬率:{row["六個月"]},\n三個月報酬率:{row["三個月"]}\n'
    return fund_rule_items_str


currency_list = twder.currencies()

def get_all_currencies_rates_str():
    all_currencies_rates_str = ''
    all_currencies_rates = twder.now_all()

    for currency_code, all_currency_rates in all_currencies_rates.items():
        all_currencies_rates_str += f'[{currency_code}]\n現金買入:{all_currency_rates[1]}\n現金賣出:{all_currency_rates[2]}\n即期買入:{all_currency_rates[3]}\n即期賣出:{all_currency_rates[4]}\n時間:({all_currency_rates[0]})\n'
    return all_currencies_rates_str


def get_single_currency_rate_str(user_input):
    single_currency_rate_str = ''
    single_currency_rate = twder.now(user_input)

    single_currency_rate_str += f'[{user_input}]\n現金買入:{single_currency_rate[1]}\n現金賣出:{single_currency_rate[2]}\n即期買入:{single_currency_rate[3]}\n即期賣出:{single_currency_rate[4]}\n時間:({single_currency_rate[0]})\n'
    return single_currency_rate_str


@app.route("/", methods=['GET'])
def hello():
    return 'hello heroku'


@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)
    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_input = event.message.text
    if user_input == '@台積電股價估算':
        crawl_for_stock_price_job()
        print('開始讀取資料')
        stock_item_lists = worksheet.get_all_values()
        print(stock_item_lists)
        stock_no_list = ['2330']
        for stock_item in stock_item_lists:
            stock_no = stock_item[0]
            high_price = stock_item[1]
            middle_price = stock_item[2]
            low_price = stock_item[3]
            if str(stock_no) in stock_no_list:
                latest_trade_price = twstock.realtime.get(stock_no)['realtime']['latest_trade_price']
                price_rule_message = get_check_price_rule_message(stock_no, high_price, middle_price, low_price, latest_trade_price)
                line_bot_api.reply_message(event.reply_token,TextSendMessage(text='[本益比估價法]' + '\n' + price_rule_message))
    elif user_input == '@基金列表':
        fund_list_str = ''
        for fund_name in fund_map_dict:
            fund_list_str += fund_name + '\n'
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=fund_list_str + '\n' + '請輸入基金名稱'))
    elif user_input in fund_map_dict:
        group_id = fund_map_dict[user_input]
        print('開始依照4433法則篩選...')
        fund_rule_items_str = fetch_fund_rule_items(year, month, group_id)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='===4433 法則===' + '\n' + fund_rule_items_str))
    elif user_input == '@貨幣列表':
        currency_list_str = ''
        for currency in currency_list:
            currency_list_str += currency + '\n'
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text=currency_list_str+ '\n' + '請輸入貨幣代號'))
    elif user_input == '@查詢所有匯率':
        all_currencies_rates_str = get_all_currencies_rates_str()
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text=all_currencies_rates_str))
    elif user_input in currency_list:
        single_currency_rate_str = get_single_currency_rate_str(user_input)
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text=single_currency_rate_str))
    else:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='請輸入正確指令'))

init_fund_list()

if __name__ == '__main__':
    app.run()