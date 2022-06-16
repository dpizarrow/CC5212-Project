# CC5212-Project

Group Members:

    - Alberto Abarzua
    - Juan Pablo Alvira
    - Diego Pizarro

## Overview

The main goal of the project is to figure out which artists are the most consistent, by defining a custom metric called a streak. We defined a streak as the amount of consecutive songs released by an artist that reach a certain level of popularity. This was achieved using a combination of the Pandas library for the initial cleaning of the dataset, followed by Apache Spark to process the data.

## Data

The project is based on the [8+ M. Spotify Tracks, Genre, Audio Features](https://www.kaggle.com/datasets/maltegrosse/8-m-spotify-tracks-genre-audio-features) dataset, which is hosted on Kaggle. The data was originally available as a 5.17 GB `.sqlite` file, which contained 9 tables for a total of approximately 46 million unique rows and 44 columns. After cleaning the data, we reduced the number of rows to 36.3 million, and the number of columns to 20, over a total of 5 `.tsv` files.

## Methods

Before working on procesing the data, we used the Pandas library to clean the dataset. This cleaning phase consisted of dropping all columns that contained null values, and removing any columns that were considered irrelevant for the task. During the cleaning phase, we also created small sample subsets of the original dataset, so we could try out our final script on a dataset that would not take a long time to run, which would help verify our results beforehand. The files are:

- `artists.tsv`, containing information about each artist. 
- `tracks.tsv`, which has information on each track, including its popularity
- `r_track_artist.tsv`, which has the id's of each track and artist
- `r_albums_track.tsv`, which has the id's of each album and track
- `albums.tsv`, which contains information on each album

After creating the full cleaned and sample datasets, these were uploaded to the HDFS cluster. And we began working on the Spark script. The idea behind the script is to first perform a series of maps and joins so that we could end up with the following key-value pairs:

~~~
   (artist_id, (artist_name, song_name,track_popularity,release_date))
~~~

These key-value pairs are sorted by the artist's id, followed by each songs descending release date. By adding a numerical index to each key-value pair, we have a way of knowing if each song forms a part of a streak. After this, an `aggrgateByKey`, we obtain a tuple where the key is the artist id, and the value is an array of indices, that denote which songs make a streak. The final step is to find the longest subsequence in each array to find out each artist's longest streak. The final output of the Spark job are tuples of the form: 

~~~
   (artist_name, longest_streak_length)
~~~    

## Results

When defining the minimum popularity to be 60, the top 10 artists with the longest streaks are:

|     Artist    | Streak Length |
|:-------------:|:-------------:|
|     Drake     |       14      |
|      BTS      |       14      |
|   Bad Bunny   |       14      |
|   Juice WRLD  |       14      |
| One Direction |       13      |
|  Taylor Swift |       13      |
|  Travis Scott |       12      |
|   Pop Smoke   |       11      |
| Ariana Grande |       11      |
| Justin Bieber |       11      |

Analyzing the table its possible to observe that some of the most recognized artists of the past 10 years make an appearance as the most consistent artists based on the metric we defined earlier. This is not surprising, as we expected that some of the most best-selling and popular artists would end up also being more consistent in their ability to release consecutive and also popular songs. 


## Running the script

To run the file: 

~~~
spark-submit --master spark://cluster-01:7077 streaks.py <MIN_POPULARITY> <INPUT_PATH> <OUTPUT_PATH>
~~~

If not specified, the default minimum popularity is 50. The input path should contain all the `.tsv` files specified earlier. To view the results:

~~~
hdfs dfs -cat OUTPUT_PATH/part-00000 | more
~~~