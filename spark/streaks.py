from pyspark.sql import SparkSession




if __name__ == '__main__':
    spark = SparkSession.builder.appName("Pythonlab5").getOrCreate()
    # paths
    album_path = "hdfs://cm:9000/uhadoop2022/G5/sample_data/albums.tsv"
    artists_path = "hdfs://cm:9000/uhadoop2022/G5/sample_data/artists.tsv"
    r_albums_tracks_path = "hdfs://cm:9000/uhadoop2022/G5/sample_data/r_albums_tracks.tsv"
    r_track_artist_path = "hdfs://cm:9000/uhadoop2022/G5/sample_data/r_track_artist.tsv"
    tracks_path = "hdfs://cm:9000/uhadoop2022/G5/sample_data/tracks.tsv"

    #Create RDDs
    first_elem_lambda = lambda r: r[0]

    albums_inputRDD = spark.read.text(album_path).rdd.map(first_elem_lambda)
    artists_inputRDD =spark.read.text(artists_path).rdd.map(first_elem_lambda)
    r_albums_tracks_inputRDD =spark.read.text(r_albums_tracks_path).rdd.map(first_elem_lambda)
    r_track_artist_inputRDD =spark.read.text(r_track_artist_path).rdd.map(first_elem_lambda)
    tracks_inputRDD =spark.read.text(tracks_path).rdd.map(first_elem_lambda)

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
    #join2_mapped =  join2.map(lambda x: (x[0],x[1][0],x[1][1][3],x[1][2][0],x[1][2][2],x[1][1][5],x[1][2][3]))


    """
    
            join2 == (album_id, (track_id, (disc_number	duration	explicit	name	track_number	popularity), (name	album_type	release_date popularity)) 

            join2_mapped == (album_id,track_id,track_name,album_name,release_date,track_popularity,album_popularity)  
           

    """         


    

    result = join2
 
    #Saving the results.
    fileout = "hdfs://cm:9000/uhadoop2022/G5/results_project/"
    result.saveAsTextFile(fileout)
    spark.stop()
 

