import pandas as pd
import os


########################  影片清單  ###########################

#從完整的影片清單逐項確認表格是否存在，挑出本月的節目
full_video_list = ['小麥的健康筆記','小豪出任務','中天車享家_朱朱哥來聊車','世界越來越盧','民間特偵組',
'全球政經週報','老Z調查線','你的豪朋友','宏色封鎖線_宏色禁區','金牌特派','阿比妹妹','政治新人榜','洪流洞見',
'流行星球','食安趨勢報告','真心話大冒險','愛吃星球','新聞千里馬','新聞龍捲風','詩瑋愛健康',
'詭案橞客室','嗶!就是要有錢','窩星球','綠也掀桌','與錢同行','論文門開箱','鄭妹看世界',
'螃蟹秀開鍘','獸身男女','靈異錯別字_鬼錯字','琴謙天下事']

video_list = []
for video in full_video_list:
    videofilepath = "video_table/table_{}.csv".format(video)
    if os.path.isfile(videofilepath):
        video_list.append(video)

########################  bottom  ############################




########################  通用函式  ###########################

def table_generator(table):
    #訂閱
    sub_yes = table["ViewsSubs"][1]
    sub_no = table["ViewsSubs"][2]
    totalsub = table["ViewsSubs"][0]
    yes = round(sub_yes/totalsub,2)
    no = round(sub_no/totalsub,2)
    subs_table = {
        "未訂閱":[yes],
        "已訂閱":[no]    
    }
    subs_table = pd.DataFrame(subs_table)

    #性別
    gender_table = {
        "女":[table["Views (%)gender"][0]],
        "男":[table["Views (%)gender"][1]]    
    }
    gender_table = pd.DataFrame(gender_table)

    #年齡，只要確認沒有該年齡範圍，就創建一個值為0的年齡欄位
    global age_table
    age_table = {"age range":['nan']}
    age_table = pd.DataFrame(age_table)

    #13–17歲
    if '13–17 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='13–17 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"13–17 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '13-17')
    else:
        age_table_a = {"13–17 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '13-17')

    #18–24歲
    if '18–24 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='18–24 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"18–24 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '18–24')
    else:
        age_table_a = {"18–24 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '18–24')

    #25–34歲
    if '25–34 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='25–34 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"25–34 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '25–34')
    else:
        age_table_a = {"25–34 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '25–34')

    #35–44歲
    if '35–44 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='35–44 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"35–44 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '35–44')
    else:
        age_table_a = {"35–44 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '35–44')

    #45–54歲
    if '45–54 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='45–54 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"45–54 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '45–54')
    else:
        age_table_a = {"45–54 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '45–54')

    #55–64歲
    if '55–64 years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='55–64 years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"55–64 years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '55–64')
    else:
        age_table_a = {"55–64 years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '55–64')

    #65歲+
    if '65+ years' in table["Viewer age"].values:
        age = table["Views (%)"][table["Viewer age"]=='65+ years']
        age = pd.DataFrame(age)
        age = age.reset_index()
        age_table_a = {"65+ years":[age['Views (%)'][0]]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '65+')
    else:
        age_table_a = {"65+ years":[0]}
        age_table_a = pd.DataFrame(age_table_a)
        age_table = age_table.join(age_table_a,rsuffix = '65+')
    
    age_table = age_table.drop('age range', axis = 1)

    #地區
    #抓取特定地區的觀看人數及總人數，若沒有則為0，然後再做比例計算
    peo_table = {"total":[table['ViewsGeography'][0]]}
    peo_table = pd.DataFrame(peo_table)

    country_list = ['TW','US','CA','SG','MY','CN','MO','HK']
    for country in country_list:
        if country in table['Geography'].values:
            peo = table['ViewsGeography'][table['Geography'] == country]
            peo = pd.DataFrame(peo)
            peo = peo.reset_index()
            peo = peo['ViewsGeography'][0]
            peo_table_1 = {country:[peo]}
            peo_table_1 = pd.DataFrame(peo_table_1)
            peo_table = peo_table.join(peo_table_1,rsuffix = country)
        else:
            peo = 0
            peo_table_1 = {country:[peo]}
            peo_table_1 = pd.DataFrame(peo_table_1)
            peo_table = peo_table.join(peo_table_1,rsuffix = country)

    geo_tw = peo_table['TW'][0]/peo_table['total'][0]
    geo_uc = (peo_table['US'][0]+peo_table['CA'][0])/peo_table['total'][0]
    geo_sm = (peo_table['SG'][0]+peo_table['MY'][0])/peo_table['total'][0]
    geo_cn = (peo_table['CN'][0]+peo_table['HK'][0]+peo_table['MO'][0])/peo_table['total'][0]
    geo_other = 1-(geo_tw+geo_uc+geo_sm+geo_cn)

    geo_table = {
        "台灣地區":[geo_tw],
        "美加地區":[geo_uc],
        "星馬地區":[geo_sm],
        "中港澳地區":[geo_cn],
        "其他地區":[geo_other]
        }
    geo_table = pd.DataFrame(geo_table)


    #合併欄位
    table_sibaraku = subs_table.join(gender_table,rsuffix = 'nan1')

    table_sibaraku = table_sibaraku.join(age_table,rsuffix = 'nan2')
    table_sibaraku = table_sibaraku.join(geo_table,rsuffix = 'nan3')

    table_x = table[['youtuber','name','總影片數']].head(1)
    table_x = table_x.join(table_sibaraku,rsuffix = 'nan4')

    # 加入收益總和
    revenue = table['Your estimated revenue (USD)'].head(1)
    revenue = revenue.round(0)
    table_finish = table_x.join(revenue,rsuffix = 'Premium')

    # 部分欄位轉換為百分比顯示
    table_finish['未訂閱'] = str(int(table_finish['未訂閱']*100))+'%'
    table_finish['已訂閱'] = str(int(table_finish['已訂閱']*100))+'%'
    table_finish['女'] = str(int(round(table_finish['女'])))+'%'
    table_finish['男'] = str(int(round(table_finish['男'])))+'%'
    table_finish['13–17 years'] = str(int(round(table_finish['13–17 years'])))+'%'
    table_finish['18–24 years'] = str(int(round(table_finish['18–24 years'])))+'%'
    table_finish['25–34 years'] = str(int(round(table_finish['25–34 years'])))+'%'
    table_finish['35–44 years'] = str(int(round(table_finish['35–44 years'])))+'%'
    table_finish['45–54 years'] = str(int(round(table_finish['45–54 years'])))+'%'
    table_finish['55–64 years'] = str(int(round(table_finish['55–64 years'])))+'%'
    table_finish['65+ years'] = str(int(round(table_finish['65+ years'])))+'%'
    table_finish['台灣地區'] = str(int(round(table_finish['台灣地區']*100)))+'%'
    table_finish['美加地區'] = str(int(round(table_finish['美加地區']*100)))+'%'
    table_finish['星馬地區'] = str(int(round(table_finish['星馬地區']*100)))+'%'
    table_finish['中港澳地區'] = str(int(round(table_finish['中港澳地區']*100)))+'%'
    table_finish['其他地區'] = str(int(round(table_finish['其他地區']*100)))+'%'
    
    return table_finish

########################  bottom  ############################




########################  表格輸出  ###########################

aud = {
    'youtuber':[],'name':[],'總影片數':[],'未訂閱':[],'已訂閱':[],'女':[],'男':[],
    '13–17 years':[],'18–24 years':[],'25–34 years':[],'35–44 years':[],'45–54 years':[],
    '55–64 years':[],'65+ years':[],'台灣地區':[],'美加地區':[],'星馬地區':[],
    '中港澳地區':[],'其他地區':[],'Your estimated revenue (USD)':[]
    }
aud = pd.DataFrame(aud)

for video in video_list:
    table = pd.read_csv('video_table/table_{}.csv'.format(video),encoding = 'utf-8-sig')
    aud_1 = table_generator(table)
    aud = pd.concat([aud,aud_1],ignore_index=True)

aud = aud.sort_values(by = ['Your estimated revenue (USD)'],ascending = False)
aud = aud.reset_index(drop = True)
aud.index+=1
#aud = aud.drop('YouTube ad revenue (USD)',axis = 1)                   # 給員工看的要刪除，但也可以從excel刪除

print(aud)

filepath = "輸出報表"
if os.path.isdir(filepath):
    print('directory CHECK OK')
else:
    os.mkdir("輸出報表")

aud.to_csv('輸出報表/table_audience.csv',encoding = 'utf-8-sig')

########################  bottom  ############################

print('===============  audience_data successful  ===============')