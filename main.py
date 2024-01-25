import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
from pymongo import MongoClient
from googleapiclient.discovery import build
from PIL import Image
from sqlalchemy import create_engine
import urllib.parse


api_key = "AIzaSyDRcsElw4U7Nck2_gaKE9IH_EBJ7ootkKI"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

# Construct the MongoDB URI with encoded username and password
uri = f"mongodb+srv://Ram:Ram@cluster0.xk8hc.mongodb.net/"
client = MongoClient(uri)
db = client["youtube_warehousing"]
collection = db["youtube"]

conn = sql.connect(host="localhost", user="root", password="Randstad@12345", database="youtube_warehousing")
cursor = conn.cursor()
password = urllib.parse.quote("Randstad@12345")
con_str = f'mysql+pymysql://root:{password}@localhost:3306/youtube_warehousing'
engine = create_engine(con_str)


icon = Image.open("Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing App",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by Ram"""})

with st.sidebar:
    
    selected = option_menu(None, ["Overview","Extract and Transform","Insights"], 
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#2e2696"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "5000px"},
                                   "nav-link-selected": {"background-color": "#2e2696"}})


st.title("YouTube Data Warehousing App")


def get_channel_data(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

def get_playlist_data(channel_id):

    ch_id = channel_id[0]
    request = youtube.playlists().list(part="snippet", channelId= ch_id, maxResults=50)
    response = request.execute()

    playlists = []

    # Process and print the playlist data
    for item in response["items"]:
        playlist_details = dict(Playlist_id = item["id"], Playlist_title = item["snippet"]["title"], Channel_id = ch_id)
        playlists.append(playlist_details)

    return playlists


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_data(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics'].get('viewCount', 0),
                                Likes = video['statistics'].get('likeCount',0),
                                Comments = video['statistics'].get('commentCount',0),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_data(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


if selected == "Overview":
    # Title Image
    
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :green[Technologies used] : Python, MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[About this App] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col1.image("youtubeMain.png")
    
# EXTRACT and TRANSFORM PAGE
if selected == "Extract and Transform":
    tab1,tab2 = st.tabs([" EXTRACT ", " TRANSFORM "])
    
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter the Channel_ID below :")
        ch_id = st.text_input("Channel ID:").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_data(ch_id)
            st.write(f'#### Extracted data from :red["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):

            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_data(ch_id)
                playlist_details = get_playlist_data(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_data(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_data(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.playlist_details
                collections2.insert_many(playlist_details)

                collections3 = db.video_details
                collections3.insert_many(vid_details)

                collections4 = db.comments_details
                collections4.insert_many(comm_details)

                st.success("Successfully Uploaded to MongoDB!!")

    # FUNCTION TO GET CHANNEL NAMES FROM MONGODB
    def channel_names():   
        ch_name = []
        for i in db.channel_details.find():
            ch_name.append(i['Channel_name'])
        return ch_name         
      
    # TRANSFORM TAB
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Choose a channel to transform it's data to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options= ch_names)

        for i in db.channel_details.find():
            if(i['Channel_name']==user_inp):
                ch_id = i['Channel_id']

        def insert_into_channels():
            data = list(db.channel_details.find({"Channel_name" : user_inp},{'_id':0}))
            df = pd.DataFrame(data)
            df['Views'] = df['Views'].astype(int)
            df['Total_videos'] = df['Total_videos'].astype(int)
            df['Subscribers'] = df['Subscribers'].astype(int)
            df.to_sql(name='channel', con=engine, if_exists='append', index=False)

        
        def insert_into_playlists():
            data = list(db.playlist_details.find({"Channel_id" : ch_id},{'_id':0}))
            df = pd.DataFrame(data)
            df.to_sql(name='playlists', con=engine, if_exists='append', index=False)

        
        def insert_into_videos():
            data = list(db.video_details.find({"Channel_id" : ch_id},{'_id':0}))
            df = pd.DataFrame(data)
            df['Views'] = df['Views'].astype(int)
            df['Likes'] = df['Likes'].astype(int)
            df['Comments'] = df['Comments'].astype(int)
            df['Favorite_count'] = df['Favorite_count'].astype(int)
            df.to_sql(name='videos', con=engine, if_exists='append', index=False)


        def insert_into_comments():
            
            # to create an empty dataframe 'df2'
            data2 = list(db.comments_details.find())
            df1 = pd.DataFrame(data2)
            columns_for_df2 = [col for col in df1.columns if col != '_id']
            df2 = pd.DataFrame(columns=columns_for_df2)

            # to get data from video_details
            data1 = list(db.video_details.find({"Channel_id" : ch_id},{'_id':0}))
            df = pd.DataFrame(data1)
            for row in df.values:
                v_id = row[df.columns.get_loc('Video_id')]
                data3 = list(db.comments_details.find({"Video_id" : v_id},{'_id':0}))
                df2 = pd.concat([df2,pd.DataFrame(data3)], ignore_index=True)
            df2['Like_count'] = df2['Like_count'].astype(int)
            df2['Reply_count'] = df2['Reply_count'].astype(int)
            df2.to_sql(name='comments', con=engine, if_exists='append', index=False)


        if st.button("Submit"):
            try:
                insert_into_channels()
                insert_into_playlists()
                insert_into_videos()
                insert_into_comments()
                conn.commit()
                st.success("Transformation to MySQL Successful!!!")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
            finally:
                engine.dispose()
            
# VIEW PAGE
if selected == "Insights":

    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT Title AS Title, Channel_name AS Channel_Name FROM videos ORDER BY Channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT Channel_name 
        AS Channel_Name, Total_videos AS Total_Videos
                            FROM channel
                            ORDER BY Total_videos DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= cursor.column_names[0],y= cursor.column_names[1])
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT Channel_name AS Channel_name, Title AS Title, Views AS Views 
                            FROM videos
                            ORDER BY Views DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_name AS Channel_Name,Title AS Title,Likes AS Likes_Count 
                            FROM videos
                            ORDER BY Likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT Title AS Title, Likes AS Likes_Count
                            FROM videos
                            ORDER BY Likes DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_name AS Channel_Name, Views AS Views
                            FROM channel
                            ORDER BY Views DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT Channel_name AS Channel_Name
                            FROM videos
                            WHERE Published_date LIKE '2022%'
                            GROUP BY Channel_name
                            ORDER BY Channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.write("### :green[Average video duration for channels :]")
        cursor.execute("""SELECT Channel_name, 
                        SUM(Duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT Channel_name, 
                            CASE
                                WHEN Duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'H', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'H', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'T', -1)))
                                END AS Duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY Channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_name AS Channel_Name,Video_id AS Video_ID,Comments AS Comments
                            FROM videos
                            ORDER BY Comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=cursor.column_names[1],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    
