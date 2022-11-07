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



########################  通用函式  ##########################

# 抓取需要的欄位第一筆的統計資料
def table_ytbp(table):

    # 抓取需要的表格欄位
    table = table[
        ['youtuber','name','Your estimated ad revenue (USD)','Your YouTube Premium revenue (USD)',
        'Your transaction revenue (USD)','Your estimated revenue (USD)','Views','Watch time (hours)',
        'Subscribers','新進會員數','總影片數']
        ].head(1)

    # 更改欄位順序，測試功能用途，不啟用
    '''
    table = table.reindex(columns=['youtuber','name','Your estimated ad revenue (USD)',
    'Your YouTube Premium revenue (USD)','Your transaction revenue (USD)','Your estimated revenue (USD)',
    'Views','Watch time (hours)','Subscribers','新進會員數','總影片數'])
    '''

    # 特定欄位四捨五入
    table = table.round({
        'Your estimated ad revenue (USD)':0,'Your estimated revenue (USD)':0,
        'Your YouTube Premium revenue (USD)':1,
        'Your transaction revenue (USD)':1,'Watch time (hours)':0})
    return table

########################  bottom  ############################



########################  表格輸出  ###########################

ytbp = {
    'youtuber':[],'name':[],'Your estimated ad revenue (USD)':[],'Your YouTube Premium revenue (USD)':[],
    'Your transaction revenue (USD)':[],'Your estimated revenue (USD)':[],'Views':[],'Watch time (hours)':[],
    'Subscribers':[],'新進會員數':[],'總影片數':[]
    }
ytbp = pd.DataFrame(ytbp)

for video in video_list:
    table = pd.read_csv('video_table/table_{}.csv'.format(video),encoding = 'utf-8-sig')
    ytbp_1 = table_ytbp(table)
    ytbp = pd.concat([ytbp,ytbp_1],ignore_index=True)

ytbp = ytbp.sort_values(by = ['Your estimated revenue (USD)'],ascending = False)
ytbp = ytbp.reset_index(drop = True)
ytbp.index+=1


filepath = "輸出報表"
if os.path.isdir(filepath):
    print('directory CHECK OK')
else:
    os.mkdir("輸出報表")

print(ytbp)
ytbp.to_csv('輸出報表/table_ytb_performance.csv',encoding = 'utf-8-sig')


########################  bottom  ############################


print('===============  ytb_performance successful  ===============')
