from pyspark.sql import SparkSession
from heapq import merge
import sys




# Seqfun
def first_fun(prev,cur):
        a_name,s_name,pop,d,index = cur
        if int(pop.decode())>MIN_POP: # cumple
                prev.append(int(index))
        return prev

# Combfun
def second_fun(prev,cur):
    return list(merge(prev,cur))

    
def streak_creator(L):
    max_count = 0
    cur_count = 1
    if (len(L) == 1):
        return 1
    for i,elem in enumerate(L):
        if (i == len(L)-1):
            break
        if elem+1 == L[i+1]:
            cur_count+=1
        if (cur_count>max_count):
            max_count = cur_count
            cur_count = 0
    return max_count+1
        



if __name__ == '__main__':
        """args:   <MIN_POP> <PATH_INPUTS>  <PATH_OUTPUTS>"""

        # INPUT  PATHS.
        BASE = "hdfs://cm:9000/uhadoop2022/G5/sample_data/"



        # OUTPUT PATH.
        FILEOUT = "hdfs://cm:9000/uhadoop2022/G5/results_project/"

        MIN_POP = 30
        
        if len(sys.argv) == 2 :
                MIN_POP = float(sys.argv[1])
        elif len(sys.argv) ==4:
                MIN_POP = float(sys.argv[1])
                BASE = sys.argv[2]
                FILEOUT = sys.argv[3]

        spark = SparkSession.builder.appName("StreakFinder").getOrCreate()


        #Create RDDs
        first_elem_lambda = lambda r: r[0]
        
        ALBUM_PATH = BASE+"/albums.tsv"
        ARTIST_PATH = BASE + "/artists.tsv"
        R_ALBUMS_TRACKS_PATH = BASE + "/r_albums_tracks.tsv"
        R_TRACK_ARTIST_PATH = BASE + "/r_track_artist.tsv"
        TRACKS_PATH = BASE + "/tracks.tsv"

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


                join2_mapped == (album_id,track_id,track_name,album_name,release_date,track_popularity,album_popularity)  
                

        """         
        pre_join3 = join2_mapped2.map(lambda val: (val[1],tuple([val[0]]+[val[2:]] )))
        # track_id	artist_id
        join3 = r_track_artist_lines.join(pre_join3)
        join3_map = join3.map((lambda val: (val[1][0],val[0],val[1][1][0])+ val[1][1][1]))

        artirst_zip = artists_lines.map(lambda val: (val[1],tuple([val[0]]+[val[2:]] ))).cache()
        pre_join4 = join3_map.map(lambda val: (val[0],tuple(val[1:])))
        join4 = pre_join4.join(artirst_zip)



        filtered_data = join4.map(lambda val: (val[0],val[1][1][0],val[1][0][2],val[1][0][5],val[1][0][4]))
        filtered_data_map = filtered_data.map(lambda val: (val[0],tuple(val[1:])))
        filtered_data_map_order = filtered_data_map.sortBy(lambda val : (val[0],val[1][3]))
        filtered_index = filtered_data_map_order.zipWithIndex()
        filtered_final = filtered_index.map(lambda val: (val[0][0],(val[0][1])+(val[1],)))

        """
        filtered_data_map =   (artist_id,(artist_name, song_name,track_popularity,release_date))
        """
        init_values = []
        streaks_pre = filtered_final.aggregateByKey(init_values,first_fun,second_fun)
        streaks_no_name = streaks_pre.map(lambda val: (val[0],streak_creator(val[1])))
        streaks_no_map = streaks_no_name.join(artirst_zip)
        streaks_no_sort = streaks_no_map.map(lambda val: (val[1][1][0],val[1][0]))
        streaks  = streaks_no_sort.sortBy(lambda val : val[1],False)
        result = streaks


        #Saving the results.
        result.saveAsTextFile(FILEOUT)
        spark.stop()


