import pymongo as pm
import pandas as pd
import psycopg2 as pg
from googleapiclient.discovery import build
import numpy as np
apikey="AIzaSyBfY0mQIW8X6yI4aK-dBbco7YwZ1p83L6c"

channelids=["UCYwrS5QvBY_JbSdbINLey6Q","UC9LjrPL1bLjJ2oIU3NSdcMQ" ]

youtube=build("youtube",'v3',developerKey=apikey)
request = youtube.channels().list(
       part="snippet,contentDetails,statistics",
        id=','.join(channelids))
response = request.execute()
#print(response)


#To get channel stats
def get_channel_stats(youtube,channelids):
  request = youtube.channels().list(
       part="snippet,contentDetails,statistics",
        id=','.join(channelids))
  alldata=[]
  response = request.execute()

# to get every channel data

  for i in range(len(response['items'])):

    data=dict(channel_name=response['items'][i]['snippet']['title'],
            subscribers=response['items'][i]['statistics']['subscriberCount'],
            Total_videos=response['items'][i]['statistics']['videoCount'],
            views=response['items'][i]['statistics']['viewCount']
            )
    alldata.append(data)
   # print(alldata)
  return alldata

#g=get_channel_stats(youtube,channelids)
dicdata=get_channel_stats(youtube,channelids)
#print(dicdata)
channelstats=get_channel_stats(youtube,channelids)
channel_data=pd.DataFrame(channelstats)


# mdb=pm.MongoClient('mongodb://Ashok:ashokroot@ac-irmhljq-shard-00-00.oixpusa.mongodb.net:27017,ac-irmhljq-shard-00-01.oixpusa.mongodb.net:27017,ac-irmhljq-shard-00-02.oixpusa.mongodb.net:27017/?ssl=true&replicaSet=atlas-tizwhv-shard-0&authSource=admin&retryWrites=true&w=majority')
#
# db=mdb['1yt_demo']
# col=db['details']
#
# col.insert_many(dicdata)
# print(dicdata)
# print("Success")


#sql Connectivity
ak=pg.connect(host='localhost', user='postgres', password='ashokroot', database='ak_1')
cursor=ak.cursor()

def sqldata(channel_data):
    for i in channel_data.loc[channel_data.index].values:
        query='insert into table_yt values(%s,%s,%s,%s);'
        res=[]
        cursor.execute(query,i)
        ak.commit()
        print('sussessfully inserted pg')
        # print(res)
#sqldata(channel_data)

print(channel_data)
print(channel_data.dtypes)
