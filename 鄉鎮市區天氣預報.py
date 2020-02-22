import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import pymysql
import datetime


#處理鄉鎮市區代碼的檔案
#以pandas讀入鄉鎮市區代碼的excel檔
df = pd.read_excel('鄉鎮市區代碼.xlsx')
#print出前五列
# print(df.head())

#取得標題
header = df.iloc[2]
df1 = df[3:].copy()
df1 = df1.rename(columns = header)
# print(df1.head())

#去掉不需要的欄位
df2 = df1.drop(columns={'縣市代碼','村里代碼','村里代碼','村里名稱','村里名稱'}, axis = 1)
# print(df2.head())

#去除重複的區里代碼
df3 = df2.drop_duplicates()
#存成csv檔
df3.to_csv('district.csv', encoding='big5', index=False)



#將抓資料的流程寫進方法裡，並帶參數:district_code
def getInfo(district_code):
    
    headers = {
    'Accept': 'text/html, */* q = 0.01',
    'Referer': 'https: // www.cwb.gov.tw/V8/C/W/Town/Town.html?TID = 6301000',
    'Sec-Fetch-Dest': 'empty',
    'X-Requested-With': 'XMLHttpRequest',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36'
    }
    url_front = 'https://www.cwb.gov.tw/V8/C/W/Town/MOD/3hr/' 
    url_back = '_3hr_PC.html?T='
    x = datetime.datetime.now()
    today = str(x.year) + str(x.month)  + str(x.day) + str(x.hour) + "-0"
    url = url_front + district_code + url_back + today
    html = requests.get(url, headers=headers)
    html.encoding = 'utf-8'
    soup = BeautifulSoup(html.text, 'html.parser')
    res = soup.find('div', {'class': 'table-responsive three-hr-forecast'})
    
    # #抓時間&日期
    times = []
    dates = []

    # 抓溫度
    temp_datas = []

    # 天氣狀況
    Wxs = []

    # 降雨機率 (有使用colspan來合併儲存格，要另外跑迴圈)    
    rain_datas = res.select('.rain_wrap td')
    pops = []
    # 重複次數
    rep = 0
    for i in range(0, len(rain_datas)):
        td = rain_datas[i]
        if td.has_attr('colspan'):
            rep = int(td.attrs['colspan'])
        else:
            rep = 1
        for j in range(0, rep):
            pops.append(td.text)


    # 相對溼度
    wets = []
    wet_res = res.find_all('tr', {'class': 'uvi_wrap'})[1]
    wet_datas = wet_res.select('td')


    # 舒適度
    feels = []
    feel_res = res.find_all('tr', {'class': 'uvi_wrap'})[3]
    feel_datas = feel_res.select('td')

    for i in range(0, len(res.select('.t'))):

        times.append(res.select('.t')[i].text)
        dates.append(res.select('.d')[i].text)
        temp_datas.append(res.find_all('span', {'class': 'tem-C is-active'})[i].text)
        Wxs.append(res.find_all('img')[i]['alt'])
        wets.append(wet_datas[i].text)
        feels.append(feel_datas[i].text)
    
    # 先把資料寫進DataFrame
    columns = ['日期', '時間', '溫度', '天氣狀況', '降雨機率', '相對溼度', '舒適度']
    df = pd.DataFrame(columns=columns)
    
    df['日期'] = dates
    df['時間'] = times
    df['溫度'] = temp_datas
    df['天氣狀況'] = Wxs
    df['降雨機率'] = pops
    df['相對溼度'] = wets
    df['舒適度'] = feels

    return df

#將匯入資料庫的流程寫進writeMySQL方法，並帶參數(district_name, district_code, weather_df)
def writeMySQL(district_name, district_code, weather_df):
    
    # 建立資料庫連線
    try:
        conn = pymysql.connect('localhost', port=3306, user='root', password='', charset='utf8', db='weather')
        cursor = conn.cursor()

    except Exception as e:
        print('資料庫連線錯誤:',e)

    # 查詢資料表，若資料表不存在則建立資料表
    try:
        sql = "SELECT * FROM dict_weather LIMIT 1;"
        cursor.execute(sql)
    except:
        sql = '''
        CREATE TABLE IF NOT EXISTS dict_weather(
            鄉鎮區名稱 VARCHAR(20) ,
            鄉鎮區代碼 INT, 
            日期 VARCHAR(10) ,
            時間 VARCHAR(5) ,
            溫度 INT,
            天氣狀況 VARCHAR(20),
            降雨機率 VARCHAR(5),
            相對溼度 VARCHAR(5),
            舒適度 VARCHAR(255),
            PRIMARY KEY(鄉鎮區名稱,日期,時間)
        )
        '''
        cursor.execute(sql)

    #逐行處理
    for i in weather_df.index:  
        district_name_tuple = (district_name,)
        district_code_tuple = (district_code,)
        values = district_name_tuple + district_code_tuple+ tuple(weather_df.loc[i],) 
        sql = "INSERT INTO `dict_weather` (`鄉鎮區名稱`,`鄉鎮區代碼`,`日期`,`時間`,`溫度`,`天氣狀況`,`降雨機率`,`相對溼度`,`舒適度`) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % values     
        print(sql)
        cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()
    print('資料處理完成')

#主程式
if __name__ == "__main__":

    with open('district.csv', newline='') as csvfile:
        #將各鄉鎮市區代碼檔案的資料，轉換成字典
        rows = csv.DictReader(csvfile)    
        for row in rows: 
            #分別取出檔案中的代碼及各區名稱       
            district_code = row['區里代碼'].strip()
            district_name = row['縣市名稱'] + row['區鄉鎮名稱']
            #getInfo方法return出來的df以weather_df接收
            weather_df = getInfo(district_code)
            print(weather_df)
            #呼叫writeMySQL方法，將資料匯入資料庫
            writeMySQL(district_name, district_code, weather_df)
            
