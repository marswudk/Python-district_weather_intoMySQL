# Python-district_weather_intoMySQL
取得中央氣象局各鄉鎮市區天氣預報，並匯入資料庫中(MySQL)

專題目標：
以鄉鎮市區代碼查詢各區天氣預報，取得所需的天氣預報資料後，將資料匯入MySQL中，可供他用。
實作流程：
先以單一區的天氣資料為目標，待取得後再取得全部368區的天氣資料。


1. 下載行政院主計總處的各鄉鎮市區代碼，並留下代碼及鄉鎮市區名稱，再存成csv檔

2. 先分析單一區的網頁(以內湖區 TID=6301000)

3. 降雨機率有使用colspan來合併儲存格，需另外處理
```
# 降雨機率 (有使用colspan來合併儲存格，要另外跑迴圈，colspan=2，則讓該筆資料重複填寫兩次)

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

```

4. 取得需要的資料後，將所有資料存進DataFrame中

5. 建立資料庫連線，如資料表不存在則創建資料表

6. 逐行處理每一筆資料，將資料放進SQL中
```
for i in weather_df.index:  
        district_name_tuple = (district_name,)
        district_code_tuple = (district_code,)
        values = district_name_tuple + district_code_tuple+ tuple(weather_df.loc[i],) 
        sql = "INSERT INTO `dict_weather` (`鄉鎮區名稱`,`鄉鎮區代碼`,`日期`,`時間`,`溫度`,`天氣狀況`,`降雨機率`,`相對溼度`,`舒適度`) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % values     
        print(sql)
        cursor.execute(sql)
```

7. 以鄉鎮市區代碼的csv檔做迴圈，跑完所有368個區，取得各區資料，並存入資料庫中
```
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
```