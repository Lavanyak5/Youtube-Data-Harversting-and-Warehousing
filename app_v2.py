# Proper code for YouTube API

# [Youtube API libraries]
import googleapiclient.discovery
from googleapiclient.discovery import build

# [File handling libraries]
import json
import re

# [MongoDB]
import pymongo

# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

# [pandas, numpy]
import pandas as pd
import numpy as np

# [Dash board libraries]
import streamlit as st
import plotly.express as px

st.set_page_config(layout = "wide")

st.title(":blue[Data Science and Analytics for Youtube Data using Youtube API]")
st.header(":green[Import Channel Data]")
inp_channel_id = st.text_input(":red[Enter the Channel Id:]")
st.write("Enter the channel Id to get all relavent details of a channel")
Get_data = st.button('**Start Data Extract and Store**')

# Define Session state to Get data button
if "Get_state" not in st.session_state:
    st.session_state.Get_state = False
if Get_data or st.session_state.Get_state:
    st.session_state.Get_state = True
    
def youtube_api_call(channel_id):
    # Proper code for YouTube API
  api_key = "AIzaSyCfQaSfcFHrQKDJFGESndI-LsJbuprDZ4k"
  youtube = build('youtube', 'v3', developerKey=api_key)
  next_page_token = None

  channels_response = youtube.channels().list(part='contentDetails,snippet,statistics,status',id=channel_id).execute()
  channel_data = {}
  Channel_Name = ''
  get_channel_Id = ''
  
  if 'items' not in channels_response:
    return None
  
  for channels in channels_response['items']:
    channel_data['Channel_Details'] = {'Channel_Id': channels['id'], 'Channel_Name':channels['snippet']['title'], 'Video_count':channels['statistics']['videoCount'], 'Channel_views':channels['statistics']['viewCount'],
                    'Channel_desc':channels['snippet']['description'], 'Subscriber_count':channels['statistics']['subscriberCount'], 'Total_views':channels['statistics']['viewCount'], 'Channel_status': channels['status']['privacyStatus']}

    Channel_Name = channels['snippet']['title']
    get_channel_Id = channels['id']
  # print(channel_data) correct order
  playlistItems_response = youtube.playlistItems().list(part ='snippet,contentDetails,status',playlistId=channel_id[:1]+'U'+channel_id[2:],maxResults=50,pageToken=next_page_token).execute() # Channel Id

  video_id = []
  playlist_data = {}
  plylst_idx = 0
  ply_lst_id = []
  for playlistItems in playlistItems_response['items']:
    playlist_data[f"Pl_id_{plylst_idx+1}"] = {"Play_List_id": playlistItems['id'], "Playlist_status": playlistItems['status']['privacyStatus']}
    plylst_idx += 1
    # Get video IDs
    ply_lst_id.append(playlistItems['id']) #This list is only used in Data Frame Section
    video_id.append(playlistItems['contentDetails']['videoId'])

    next_page_token = playlistItems_response.get('nextPageToken')

    if not next_page_token:
      break
      
  videos = {}
  for i,vid in enumerate(video_id):
    # Get video details
    videos_request = youtube.videos().list(part='snippet, statistics, contentDetails',id=vid)
    videos_response = videos_request.execute()
    video_items = videos_response['items'][0]
    # print(video_items)
    video_id = video_items['id']
    v_channel_id = video_items['snippet']['channelId']
    video_name = video_items['snippet']['title']
    video_description = video_items['snippet']['description']
    published_at = video_items['snippet']['publishedAt']
    view_count = video_items['statistics']['viewCount']
    like_count = video_items['statistics'].get('likeCount', 0)
    dislike_count = video_items['statistics'].get('dislikeCount', 0)
    comment_count = video_items['statistics'].get('commentCount', 0)
    comments = 'Unavailable'

    comment_request = youtube.commentThreads().list(part='snippet',maxResults=5,textFormat="plainText", videoId=vid)
    comment_response = comment_request.execute()
    # print(comment_response)
    if comment_response['items'] is not None:
      comments = {}
      for index, comment_thread in enumerate(comment_response['items']):
          comment = comment_thread['snippet']['topLevelComment']['snippet']
          comment_id = comment_thread['id']
          comment_text = comment['textDisplay']
          comment_author = comment['authorDisplayName']
          comment_published_at = comment['publishedAt']
          comments[f"Comment_Id_{index + 1}"] = {
              'Comment_Id': comment_id,
              'Comment_Text': comment_text,
              'Comment_Author': comment_author,
              'Comment_PublishedAt': comment_published_at
          }
      # print(len(comments))
    videos[f"vid_{i + 1}"] = {
    'Channel_id':v_channel_id,
    'Video_Id': video_id,
    'Video_Name': video_name,
    'PublishedAt': published_at,
    'View_Count': view_count,
    'Like_Count': like_count,
    'Dislike_Count': dislike_count,
    'Comment_Count': comment_count,
    'Comments': comments}
  # print(videos)
  channel_entire_stats = {**channel_data, **playlist_data, **videos}
#   print(channel_entire_stats)

  client = pymongo.MongoClient('mongodb://localhost:27017/')
#   print("test line 1")
  # create a database or use existing one
  mydb = client['Project_1']
#   print("test line 2")

  # create a collection
  collection = mydb['youtube_channels']
#   print("test line 3")

  # define the data to insert
  final_output_data = {'Channel_Name': Channel_Name,"Channel_data":channel_entire_stats}
#   print(pd.DataFrame([final_output_data]))

  # insert or update data in the collection
#   upload = collection.insert_one(final_output_data)
#   print("test line 5")
# insert or update data in the collection
  upload = collection.replace_one({'_id': channel_id}, final_output_data, upsert=True)

        # print the result of the insertion operation
  st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")
  #print(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")
    
  # print the result of the insertion operation
 # st.write(f"Updated document id: {upload.inserted_id}")
#   print(f"Updated document id: {upload.inserted_id}")

  # Close the connection
  client.close()
#   print("test line 7")

# ========================================   /     Data Migration (Stored data to MySQL)    /   ========================================== #

  st.header(':green[Data Migration]')

     # Connect to the MongoDB server
  client = pymongo.MongoClient("mongodb://localhost:27017/")

    # create a database or use existing one
  mydb = client['Project_1']

    # create a collection
  collection = mydb['youtube_channels']

    # Collect all document names and give them
  document_names = []
  for document in collection.find():
        document_names.append(document["Channel_Name"])
#   print(document_names)
  document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
  st.write('''Migrate to MySQL database from MongoDB.''')
  Migrate = st.button(':green[Migration Data]')

     # Define Session state to Migrate to MySQL button
  if 'migrate_sql' not in st.session_state:
        st.session_state.migrate_sql = False
  if Migrate or st.session_state.migrate_sql:
        st.session_state.migrate_sql = True

        # Retrieve the document with the specified name
  result = collection.find_one({"Channel_Name": document_name})
#   print(document_name)
#   print(result)
  client.close()
     # ----------------------------- Data conversion --------------------- #

        # Channel data json to df
  channel_details_to_sql = {
            "Channel_Name": result['Channel_Name'],
            "Channel_Id": result['_id'],
            "Video_Count": result['Channel_data']['Channel_Details']['Video_count'],
            "Subscriber_Count": result['Channel_data']['Channel_Details']['Subscriber_count'],
            "Channel_Views": result['Channel_data']['Channel_Details']['Channel_views'],
            "Channel_Description": result['Channel_data']['Channel_Details']['Channel_desc'],
            "Channel_status": result['Channel_data']['Channel_Details']['Channel_status']
            }

  channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
#   print(channel_df)

    # playlist data json to df
  playlist_list = []
  for p in range(1,51):
    playlist_tosql = {"Channel_Id": result['_id'],
                      "Playlist_Id": result['Channel_data'][f"Pl_id_{p}"]['Play_List_id'],
                      "Playlist_status": result['Channel_data'][f"Pl_id_{p}"]['Playlist_status']
                        }
    playlist_list.append(playlist_tosql)
  playlist_df = pd.DataFrame.from_dict(playlist_list)
#   print(playlist_df)

        # video data json to df
  video_details_list = []
  for i in range(1,51):
        video_details_tosql = {
#                 'Playlist_Id':result['Channel_data']['Channel_Details']['Playlist_Id'],
                'Channel_Id': result['Channel_data'][f"vid_{i}"]['Channel_id'],
                'Video_Id': result['Channel_data'][f"vid_{i}"]['Video_Id'],
                'Video_Name': result['Channel_data'][f"vid_{i}"]['Video_Name'],
                'Published_date': result['Channel_data'][f"vid_{i}"]['PublishedAt'],
                'View_Count': result['Channel_data'][f"vid_{i}"]['View_Count'],
                'Like_Count': result['Channel_data'][f"vid_{i}"]['Like_Count'],
                'Dislike_Count': result['Channel_data'][f"vid_{i}"]['Dislike_Count'],
                'Comment_Count': result['Channel_data'][f"vid_{i}"]['Comment_Count'],
                }
        video_details_list.append(video_details_tosql)
  video_df = pd.DataFrame(video_details_list)
#   print(video_df)
        # Comment data json to df
  Comment_details_list = []
  for i in range(1,51):
        comments_access = result['Channel_data'][f"vid_{i}"]['Comments']
        if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access):
            Comment_details_tosql = {
                    'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author':'Unavailable',
                    'Comment_Published_date': 'Unavailable',
                    }
            Comment_details_list.append(Comment_details_tosql)
                
        else:
            for j in range(1,3):
                Comment_details_tosql = {
                    'Video_Id': result['Channel_data'][f"vid_{i}"]['Video_Id'],
                    'Comment_Id': result['Channel_data'][f"vid_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Id'],
                    'Comment_Text': result['Channel_data'][f"vid_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Text'],
                    'Comment_Author': result['Channel_data'][f"vid_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Author'],
                    'Comment_Published_date': result['Channel_data'][f"vid_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                    }
                Comment_details_list.append(Comment_details_tosql)
  Comments_df = pd.DataFrame(Comment_details_list)
# -------------------- Data Migrate to MySQL --------------- #

        # Connect to the MySQL server
  connect = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "12345")

  # Create a new database and use
  mycursor = connect.cursor()
  mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
  mycursor.execute("SELECT count(1) FROM INFORMATION_SCHEMA.TABLES where table_name = 'channel' and table_schema = 'youtube_db'")
  insert_ind = 0 # Default No Insert

    # Logic to check if the channel data is already exists in MySQL DB.
  for result in mycursor:
        if result[0] == 1:
            mycursor.execute("SELECT count(1) FROM youtube_db.channel where Channel_name = %s",(Channel_Name,))
            for result_2 in mycursor:
                if result_2[0] >= 1:
                    insert_ind = 0
                    break
                else:
                    insert_ind = 1
                    break
        else:
            insert_ind = 1
            break
  # Close the cursor and database connection
  mycursor.close()
  connect.close()

  if insert_ind == 1: 
        # Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:12345@localhost/youtube_db', echo=False)

        # Use pandas to insert the DataFrames data to the SQL Database -> table1

        # Channel data to SQL
        channel_df.to_sql('channel', engine, if_exists='append', index=False,
        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Channel_status": sqlalchemy.types.VARCHAR(length=225),})

        # Playlist data to SQL
        playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
        dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                    "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
           "Playlist_status": sqlalchemy.types.VARCHAR(length=225),})

        # Video data to SQL
        video_df.to_sql('video', engine, if_exists='append', index=False,
        dtype = {
#       'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
        'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
        'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Published_date': sqlalchemy.types.String(length=50),
                            'View_Count': sqlalchemy.types.BigInteger,
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Dislike_Count': sqlalchemy.types.INT,
                            'Comment_Count': sqlalchemy.types.INT,})

  # Commend data to SQL
        Comments_df.to_sql('comments', engine, if_exists='append', index=False,
        dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Text': sqlalchemy.types.TEXT,
                                'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Published_date': sqlalchemy.types.String(length=50),})

  st.header(':green[Channel Data Analysis]')
  st.write ('''(Note:- To **Analyse the collection of channel data** depends on the question and to give output.)''')

# Check available channel data
  Check_channel = st.checkbox('**Display available channels**')

  if Check_channel:
   # Create database connection
      engine = create_engine('mysql+mysqlconnector://root:12345@localhost/youtube_db', echo=False)
    # Execute SQL query to retrieve channel names
      query = "SELECT DISTINCT Channel_Name FROM channel;"
      results = pd.read_sql(query, engine)
    # Get channel names as a list
      channel_names_fromsql = list(results['Channel_Name'])
    # Create a DataFrame from the list and reset the index to start from 1
      df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
    # Reset index to start from 1 instead of 0
      df_at_sql.index += 1  
    # Show dataframe
      st.dataframe(df_at_sql)

  # -----------------------------------------------------     /   Questions   /    ------------------------------------------------------------- #
  st.subheader(':green[Channels Analysis ]')
  
  # Selectbox creation
  question_tosql = st.selectbox('**Select your Question**',
  ('1. What are the names of all the videos and their corresponding channels?',
  '2. Which channels have the most number of videos, and how many videos do they have?',
  '3. What are the top 10 most viewed videos and their respective channels?',
  '4. How many comments were made on each video, and what are their corresponding video names?',
  '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
  '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
  '7. What is the total number of views for each channel, and what are their corresponding channel names?',
  '8. What are the names of all the channels that have published videos in the year 2022?',
 # '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
  '9. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')
  
  # Creat a connection to SQL
  connect_for_question = pymysql.connect(host='localhost', user='root', password='12345', db='youtube_db')
  cursor = connect_for_question.cursor()
  
  # Q1
  if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id;")
      result_1 = cursor.fetchall()
      df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
      df1.index += 1
      st.dataframe(df1)
  
  # Q2
  elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
  
      col1,col2 = st.columns(2)
      with col1:
          cursor.execute("SELECT DISTINCT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
          result_2 = cursor.fetchall()
          df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
          df2.index += 1
          st.dataframe(df2)
  
      with col2:
          fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
          fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
          fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
          st.plotly_chart(fig_vc,use_container_width=True)
  
  # Q3
  elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
  
      col1,col2 = st.columns(2)
      with col1:
          cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER BY video.View_Count DESC LIMIT 10;")
          result_3 = cursor.fetchall()
          df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
          df3.index += 1
          st.dataframe(df3)
  
      with col2:
          fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
          fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
          fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
          st.plotly_chart(fig_topvc,use_container_width=True)
  
  # Q4 
  elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER BY video.Comment_Count DESC;")
      result_4 = cursor.fetchall()
      df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
      df4.index += 1
      st.dataframe(df4)
  
  # Q5
  elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER by video.Like_Count DESC;")
      result_5= cursor.fetchall()
      df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
      df5.index += 1
      st.dataframe(df5)
  
  # Q6
  elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER BY video.Like_Count DESC,video.Dislike_Count DESC;")
      result_6= cursor.fetchall()
      df6 = pd.DataFrame(result_6,columns=['Channel Name', 'Video Name', 'Like count','Dislike count']).reset_index(drop=True)
      df6.index += 1
      st.dataframe(df6)
  
  # Q7
  elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
  
      col1, col2 = st.columns(2)
      with col1:
          cursor.execute("SELECT DISTINCT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
          result_7= cursor.fetchall()
          df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
          df7.index += 1
          st.dataframe(df7)
      
      with col2:
          fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
          fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
          fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
          st.plotly_chart(fig_topview,use_container_width=True)
  
  # Q8
  elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
      result_8= cursor.fetchall()
      df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
      df8.index += 1
      st.dataframe(df8)
  
  # Q9
 # elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
  #    cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
   #   result_9= cursor.fetchall()
    #  df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
     # df9.index += 1
      #st.dataframe(df9)
  
  # Q9
  elif question_tosql == '9. Which videos have the highest number of comments, and what are their corresponding channel names?':
      cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER BY video.Comment_Count DESC;")
      result_10= cursor.fetchall()
      df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
      df10.index += 1
      st.dataframe(df10)
  
  # SQL DB connection close
  connect_for_question.close()
  
youtube_api_call(inp_channel_id)
  