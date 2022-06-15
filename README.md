# CC5212-Project

Group Members:

    - Alberto Abarzua
    - Juan Pablo Alvira
    - Diego Pizarro

## Overview

The main goal of the project is to figure out which artists are the most consistent, by defining a custom metric called a streak. We defined a streak as the amount of consecutive songs released by an artist that reach a certain level of popularity. This was achieved using a combination of the Pandas library for the initial cleaning of the dataset, followed by Apache Spark to process the data.

## Data

The project is based on the [8+ M. Spotify Tracks, Genre, Audio Features](https://www.kaggle.com/datasets/maltegrosse/8-m-spotify-tracks-genre-audio-features) dataset, which is hosted on Kaggle. The data was originally available as a 5.17 GB `.sqlite` file, which contained 9 tables for a total of approximately 46 million unique rows. 

## Methods

Before working on procesing the data, we used the Pandas library to clean the dataset. This cleaning phase consisted of dropping all columns that contained null values, and removing any columns that were considered irrelevant for the task. During the cleaning phase, we also created small sample subsets of the original dataset, so we could try out our final script on a dataset that would not take a long time to run, which would help verify our results beforehand. After this, we were left with 5 TSV files. 

After creating the full cleaned and sample datasets, these were uploaded to the HDFS cluster. And we began working on the Spark script. The idea behind the script is to first perform a series of maps and joins so that we could end up with the following key-value pairs:

~~~
   (artist_id, (artist_name, song_name,track_popularity,release_date))
~~~

These key-value pairs are sorted by their descending release date, and grouped by the artist name. 