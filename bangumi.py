import pandas as pd
import time,re,os



start = time.time()
####################  加入通用影片資訊  #######################

report = pd.read_csv('../video_report_ctitv_V_v1-3.csv',encoding = 'utf8')   #不用utf8會缺少資料

########################  bottom  ############################



########################  通用函式  ###########################

# 從video_report 跟 content 抓取需要的欄位，再用相同的影片id作合併
def videodata(table_content,bangumi_name):
       
    # 抓取content內需要的欄位
    # 若頻道整個月只有1支影片，content的內容會沒有顯示影片ID，這時候會跳手動輸入，參閱雲端硬碟節目ID表的內容
    if len(table_content['Content']) == 1 :
        print('因YT後台無法抓取影片ID，請手動輸入{}的影片ID(請查閱google表單)：'.format(bangumi_name))
        video_id = input('輸入{}的影片ID：'.format(bangumi_name))
        id_table = {"Content":[video_id]}
        id_table = pd.DataFrame(id_table)
        new_content = table_content[[
            'Views','Watch time (hours)','Subscribers',
            'Impressions click-through rate (%)','Average percentage viewed (%)',
            'Average view duration','Likes','Comments added','Shares','Unique viewers'
            ]]        
        new_content = id_table.join(new_content,rsuffix = 'new')        
    else:
        # 去掉第一筆總計值
        table_content = table_content.tail(-1)
        new_content = table_content[[
            'Content','Views','Watch time (hours)','Subscribers',
            'Impressions click-through rate (%)','Average percentage viewed (%)',
            'Average view duration','Likes','Comments added','Shares','Unique viewers'
            ]]
           
    # 如果content的內容沒有影片ID欄位(因為全部只有一支影片，CSV檔會沒有 ID 欄位)，就手動輸入然後加入這個欄位


    # 把影片id的欄位改為 video_id
    new_content.rename(columns = {'Content':'video_id'},inplace = True)

    # 迭代 content 內的影片id，找出 video_report內相符(發布時間，影片長度...)的項目
    # 建立一個空表格，把符合的項目逐一合併進去
    table_report = {0:[]}
    table_report = pd.DataFrame(table_report)

    # 要去除字串空白部分，不然會輸出錯誤(缺項)
    new_content['video_id'] = new_content['video_id'].str.strip()
    for rows in new_content['video_id']:
        row = report[['video_id','channel_display_name','video_title','time_published','video_length'
        ]][report['video_id']==rows]
        table_report = pd.concat([table_report,row],ignore_index=True)

    table_report = table_report.drop(0,axis = 1)    # 刪除多餘的欄位
    
    # 預設影片時長單位為秒，將其轉換為分秒
    time = table_report['video_length']

    tm,ts =divmod(time,60)
    tm = pd.DataFrame(tm).astype(int)               # 將浮點數轉換為整數
    ts = pd.DataFrame(ts).astype(int)
    videotime = tm.join(ts,rsuffix = '_1')          # videotime 的第一個欄位是分，第二的欄位是秒
    
    #新增影片時長欄位，把總秒數編輯為分秒
    videotime["影片時長"] = videotime["video_length"].map(str)+ "分" + videotime["video_length_1"].map(str)+ "秒"
    videotime = videotime.drop(['video_length','video_length_1'],axis = 1)
    
    table_report = table_report.join(videotime,rsuffix = 'time')   #把新增的分秒欄位併進表格

    table_report = table_report.drop('video_length',axis = 1)      #將舊的欄位刪除
    
    # 合併 video_report 與 content 為table
    table = pd.merge(table_report,new_content, on = 'video_id',how = 'inner')
    
    # 按影片發布時間排序
    #table = table.sort_values(by = ['time_published'],ascending = True)
    table = table.sort_values(by = ['Views'],ascending = False)
    table = table.reset_index(drop = True)
    table.index+=1

    #修改單位
    table['Watch time (hours)'] = round(table['Watch time (hours)'])
    table['Impressions click-through rate (%)'] = round(table['Impressions click-through rate (%)'])
    table['Average percentage viewed (%)'] = round(table['Average percentage viewed (%)'])

    return table

########################  bottom  ############################




#建立輸出資料夾
filepath = "輸出報表/bangumi_report"
if os.path.isdir(filepath):
    print('directory CHECK OK')
else:
    os.mkdir("輸出報表/bangumi_report")



########################  影片清單  ###########################

#從完整的影片清單逐項確認表格是否存在，挑出本月的節目
full_video_list = ['小麥的健康筆記','小豪出任務','中天車享家_朱朱哥來聊車','世界越來越盧','民間特偵組',
'全球政經週報','老Z調查線','你的豪朋友','宏色封鎖線_宏色禁區','金牌特派','阿比妹妹','政治新人榜','洪流洞見',
'流行星球','食安趨勢報告','真心話大冒險','新聞千里馬','新聞龍捲風','詩瑋愛健康',
'詭案橞客室','嗶!就是要有錢','窩星球','綠也掀桌','與錢同行','論文門開箱','鄭妹看世界',
'螃蟹秀開鍘','獸身男女','靈異錯別字_鬼錯字','琴謙天下事','誰謀殺了言論自由']

video_list = []
for video in full_video_list:
    videofilepath = "video_table/table_{}.csv".format(video)
    if os.path.isfile(videofilepath):
        video_list.append(video)

########################  bottom  ############################



########################  表格輸出  ###########################

for video in video_list:
    print('匹配節目"{}"的資料...'.format(video))
    tablecontent = pd.read_csv('頻道資訊/{}/Content/Table data.csv'.format(video),encoding = 'utf8')
    table1 = videodata(tablecontent,video)
    table1.to_csv('輸出報表/bangumi_report/{}.csv'.format(video),encoding = 'utf-8-sig')   # 用utf8會亂碼，big5會缺少資料(部分無法解碼)    

########################  bottom  ############################


print('===============  bangumi_report successful  ===============')
end = time.time()
print('資料計算及報表輸出共耗時',round(end - start,2),'秒')


