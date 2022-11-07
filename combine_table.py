import pandas as pd
import os,zipfile,re

########################  通用函式  ##########################

# 同樣的表格格式，載入不同的節目，變數輸入節目名稱，用以更改路徑
def bangumi_load(bangumi):
    global tablechannel
    global tableGeography
    global tableSubs
    global tableTran
    global tableage
    global tablegender
    global tablecontent
    tablechannel = pd.read_csv('頻道資訊/{}/Channel/Table data.csv'.format(bangumi),encoding = 'utf8')
    tableGeography = pd.read_csv('頻道資訊/{}/Geography/Table data.csv'.format(bangumi),encoding = 'utf8')
    tableSubs = pd.read_csv('頻道資訊/{}/Subscription status/Table data.csv'.format(bangumi),encoding = 'utf8')

    #如果收益為0，沒有下載csv檔，就自動創建一個收益值為0的csv檔
    tranfilepath = "頻道資訊/{}/Transaction type/Table data.csv".format(bangumi)
    if os.path.isfile(tranfilepath):
        print("tableTran directory CHECK OK")
        tableTran = pd.read_csv('頻道資訊/{}/Transaction type/Table data.csv'.format(bangumi),encoding = 'utf8')
    else:
        tranfilepath_1 = "頻道資訊/{}/Transaction type".format(bangumi)
        if os.path.isdir(tranfilepath_1):
            print('tableTran directory CHECK OK')
        else:
            os.mkdir("頻道資訊/{}/Transaction type".format(bangumi))
            print('create folder')
        tran_data = {'Transaction type':['Total'],'Transactions':[0],'Your transaction revenue (USD)':[0],'新進會員數':[0]}
        tran_data = pd.DataFrame(tran_data)
        tran_data.to_csv('頻道資訊/{}/Transaction type/Table data.csv'.format(bangumi),encoding = 'utf-8-sig')
        tableTran = pd.read_csv('頻道資訊/{}/Transaction type/Table data.csv'.format(bangumi),encoding = 'utf8')

    
    tableage = pd.read_csv('頻道資訊/{}/Viewer age/Table data.csv'.format(bangumi),encoding = 'utf8')
    tablegender = pd.read_csv('頻道資訊/{}/Viewer gender/Table data.csv'.format(bangumi),encoding = 'utf8')
    tablecontent = pd.read_csv('頻道資訊/{}/Content/Table data.csv'.format(bangumi),encoding = 'utf8')


# 從交易收入裡的頻道會員抓取新會員數，做成表格，再併到 tableTran 表內
def tran_member():
    global tableTran
    if 'Channel Memberships' in tableTran['Transaction type'].values:
        #條件篩選出來的member_x 是一個 list 的狀態，不能直接取用  必須先取值再以dict形式做成 Dataframe
        member_x = tableTran['Transactions'][tableTran['Transaction type']=='Channel Memberships']        
        member_x = pd.DataFrame(member_x)        
        member_x = member_x.reset_index()
        member = {"新進會員數":[member_x['Transactions'][0]]}
        member = pd.DataFrame(member)
        tableTran = tableTran.join(member,rsuffix = 'tran')
    else:
        member = {"新進會員數":[0]}
        member = pd.DataFrame(member)
        tableTran = tableTran.join(member,rsuffix = 'tran')
    return tableTran


# 從各個csv檔抓取報表需要的欄位
def table_combine():
    table1 = tableGeography[['Geography','Views']].join(tablechannel,lsuffix = 'Geography')

    table2 = table1.join(tableSubs[['Subscription status','Views']],rsuffix = 'Subs')

    table2 = table2.join(tableTran[['Transaction type','Your transaction revenue (USD)','Transactions','新進會員數']],rsuffix = 'Tran')

    table2 = table2.join(tableage[['Viewer age','Views (%)']],rsuffix = 'age')

    table = table2.join(tablegender[['Viewer gender','Views (%)']],rsuffix = 'gender')

    #加入總影片數
    videos = len(tablecontent.index)
    if videos == 1:
        videomount = {'總影片數':[1]}
    else:
        videomount = {'總影片數':[videos-1]}
    videomount = pd.DataFrame(videomount)
    table = table.join(videomount,rsuffix = 'video')

    return table 
    

# 生成各個節目資訊表(其他函式都包在這裡面)
def table_gen(bangumi,youtuber):
    global tableTran
    bangumi_load(bangumi)
    tableTran = tran_member()
    table = table_combine()
    channel_name = {
        "name":[bangumi],
        "youtuber":[youtuber]
    }
    channel_name = pd.DataFrame(channel_name)
    table = table.join(channel_name,rsuffix = 'name')
    table.to_csv('video_table/table_{}.csv'.format(bangumi),encoding = 'utf-8-sig')


# 解壓縮所有檔案，如果壓縮檔存在就解壓縮到指定目錄，extractall會自動覆蓋原有檔案
def Channel_list(bangumi):
    dirpath = '頻道資訊/{}/壓縮檔'.format(bangumi)
    if os.path.isdir(dirpath):
        files = os.listdir(dirpath)
        for file in files:
            if re.match('Channel.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Channel'.format(bangumi))
            elif re.match('Content.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Content'.format(bangumi))
            elif re.match('Geography.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Geography'.format(bangumi))
            elif re.match('Subscription status.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Subscription status'.format(bangumi))
            elif re.match('Transaction type.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Transaction type'.format(bangumi))
            elif re.match('Viewer age.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Viewer age'.format(bangumi))
            elif re.match('Viewer gender.*.zip',file):
                zf = zipfile.ZipFile('頻道資訊/{}/壓縮檔/{}'.format(bangumi,file), 'r')
                zf.extractall(path = '頻道資訊/{}/Viewer gender'.format(bangumi))

########################  bottom  ############################





########################  輸出報表  ##########################


# 建立輸出資料夾
filepath = "video_table"
if os.path.isdir(filepath):
    print('directory CHECK OK')
else:
    os.mkdir("video_table")


# 配對頻道跟youtuber
channel = ["小麥的健康筆記",'小豪出任務','中天車享家_朱朱哥來聊車','世界越來越盧','民間特偵組',
'全球政經週報','老Z調查線','你的豪朋友','宏色封鎖線_宏色禁區','金牌特派','阿比妹妹','政治新人榜','洪流洞見',
'流行星球','食安趨勢報告','真心話大冒險','愛吃星球','新聞千里馬','新聞龍捲風','詩瑋愛健康',
'詭案橞客室','嗶!就是要有錢','窩星球','綠也掀桌','與錢同行','論文門開箱','鄭妹看世界',
'螃蟹秀開鍘','獸身男女','靈異錯別字_鬼錯字','琴謙天下事']

youtuber_name = ["麥玉潔","簡至豪","朱顯名","盧秀芳","賴麗櫻","李曉玲","周寬展","簡至豪","蔡秉宏","金汝鑫",
"林慧君","李珮瑄","洪淑芬","譚若誼","賴麗櫻","鄭亦真","張若妤","馬千惠","戴立綱","方詩瑋","何橞瑢","畢倩涵",
"何橞瑢","李珮瑄","張雅婷","何橞瑢","鄭亦真","劉盈秀","賴正鎧","賴正鎧",'周玉琴']

bangumi_list ={'bangumi':channel,'youtuber':youtuber_name}
bangumi_list = pd.DataFrame(bangumi_list)


for name,youtuber in bangumi_list.values:
    Channel_list(name)
    channelpath = "頻道資訊/{}".format(name)
    if os.path.isdir(channelpath):
        table_gen(name,youtuber)
    


'''
# 舊版本，沒有的節目，在前面加上#字號

table_gen("小麥的健康筆記",'麥玉潔')
table_gen("小豪出任務","簡至豪")
table_gen("中天車享家_朱朱哥來聊車","朱顯名")
#table_gen("世界越來越盧","盧秀芳")
table_gen("民間特偵組","賴麗櫻")

table_gen("全球政經週報","李曉玲")
table_gen("老Z調查線","周寬展")
table_gen("你的豪朋友","簡至豪")
table_gen("宏色封鎖線_宏色禁區","蔡秉宏")
table_gen("金牌特派","金汝鑫")

table_gen("阿比妹妹","林慧君")
table_gen("政治新人榜","李珮瑄")
table_gen("洪流洞見","洪淑芬")
table_gen("流行星球","譚若誼")
table_gen("食安趨勢報告","賴麗櫻")

table_gen("真心話大冒險","鄭亦真")
table_gen("愛吃星球","張若妤")
table_gen("新聞千里馬","馬千惠")
table_gen("新聞龍捲風","戴立綱")
table_gen("詩瑋愛健康","方詩瑋")

table_gen("詭案橞客室","何橞瑢")
table_gen("嗶!就是要有錢","畢倩涵")
table_gen("窩星球","何橞瑢")
table_gen("綠也掀桌","李珮瑄")
table_gen("與錢同行","張雅婷")

table_gen("論文門開箱","何橞瑢")
#table_gen("鄭妹看世界","鄭亦真")
#table_gen("螃蟹秀開鍘","劉盈秀")
table_gen("獸身男女","賴正鎧")
table_gen("靈異錯別字_鬼錯字","賴正鎧")

table_gen("琴謙天下事","周玉琴")
'''

########################  bottom  ############################


print('===============  combine successful  ===============')
