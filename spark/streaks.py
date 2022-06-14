from pyspark.sql import SparkSession


# INPUT  PATHS.
BASE = "hdfs://cm:9000/uhadoop2022/G5/sample_data/"

ALBUM_PATH = BASE+"/albums.tsv"
ARTIST_PATH = BASE + "/artists.tsv"
R_ALBUMS_TRACKS_PATH = BASE + "/r_albums_tracks.tsv"
R_TRACK_ARTIST_PATH = BASE + "/r_track_artist.tsv"
TRACKS_PATH = BASE + "/tracks.tsv"

# OUTPUT PATH.
FILEOUT = "hdfs://cm:9000/uhadoop2022/G5/results_project/"


#filtered_data_map =(artist_id,(artist_name, song_name,track_popularity,release_date))



# Seqfun
def first_fun(prev,val): 
    streak,candidate = prev
    
    

        

# Combfun
def second_fun(prev,val):
    prev_eps,prev_rating = prev
    val_eps,val_rating = val
    if (prev_rating >val_rating):
        return prev
    elif (prev_rating == val_rating):
        return (prev_eps + "|" + val_eps),prev_rating
    else:
        return val


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Pythonlab5").getOrCreate()
   

    #Create RDDs
    first_elem_lambda = lambda r: r[0]

    albums_inputRDD = spark.read.text(ALBUM_PATH).rdd.map(first_elem_lambda)
    artists_inputRDD =spark.read.text(ARTIST_PATH).rdd.map(first_elem_lambda)
    r_albums_tracks_inputRDD =spark.read.text(R_ALBUMS_TRACKS_PATH).rdd.map(first_elem_lambda)
    r_track_artist_inputRDD =spark.read.text(R_TRACK_ARTIST_PATH).rdd.map(first_elem_lambda)
    tracks_inputRDD =spark.read.text(TRACKS_PATH).rdd.map(first_elem_lambda)

    split_lambda = lambda line: line.split("\t")

    albums_lines = albums_inputRDD.map(split_lambda)
    artists_lines =artists_inputRDD.map(split_lambda)
    r_albums_tracks_lines =r_albums_tracks_inputRDD.map(split_lambda)
    r_track_artist_lines =r_track_artist_inputRDD.map(split_lambda)
    tracks_lines =tracks_inputRDD.map(split_lambda)

    """ 
    
    1. Join (Tracks,r_album_track)

    """



    tracks_zip =tracks_lines.map(lambda val: (val[0],tuple(val[1:])))
    r_albums_tracks_zip =r_albums_tracks_lines.map(lambda val: (val[1],val[0]))

    join1 = tracks_zip.join(r_albums_tracks_zip)
    join1_mapped = join1.map(lambda x: (x[0],x[1][1],x[1][0]))

    """     
    tracks_album_join = (Track) (R_trach)

    (id	disc_number	duration	explicit	name	track_number	popularity) (album_id	track_id)

    join1_mapped == (track_id , album_id ,(disc_number	duration	explicit	name	track_number	popularity))

    
    
    """




    """ 
    
    2. Join (tracks_album_join_mapped,Albums)


    """

    tracks_album_zip = join1_mapped.map(lambda val:(val[1],(val[0],val[2])))
    album_zip = albums_lines.map(lambda val: (val[0],tuple(val[1:])))
    join2 = tracks_album_zip.join(album_zip)
    join2_mapped1 =  join2.map(lambda x: (x[0],x[1][0],x[1][1]))
    join2_mapped2 =  join2_mapped1.map(lambda x: (x[0],x[1][0],x[1][1][3],x[2][0],x[2][2],x[1][1][5],x[2][3]))

    """

            join2 == (album_id, (track_id, (disc_number	duration	explicit	name	track_number	popularity), (name	album_type	release_date popularity)) 

            join2_mapped == (album_id,track_id,track_name,album_name,release_date,track_popularity,album_popularity)  
           

    """         
    pre_join3 = join2_mapped2.map(lambda val: (val[1],tuple([val[0]]+[val[2:]] )))
    # track_id	artist_id
    join3 = r_track_artist_lines.join(pre_join3)
    join3_map = join3.map((lambda val: (val[1][0],val[0],val[1][1][0])+ val[1][1][1]))

    artirst_zip = artists_lines.map(lambda val: (val[1],tuple([val[0]]+[val[2:]] )))
    pre_join4 = join3_map.map(lambda val: (val[0],tuple(val[1:])))
    join4 = pre_join4.join(artirst_zip)



    filtered_data = join4.map(lambda val: (val[0],val[1][1][0],val[1][0][2],val[1][0][5],val[1][0][4]))
    init_values = ([],[],"")
    filtered_data_map = filtered_data.map(lambda val: (val[0],tuple(val[1:])))

    #filtered_data_map =(artist_id,(artist_name, song_name,track_popularity,release_date))
    streaks = filtered_data_map.aggregateByKey(([],0.0),first_fun,second_fun)
    result = filtered_data

 
    #Saving the results.
    result.saveAsTextFile(FILEOUT)
    spark.stop()
 

