package sparkSql

import org.apache.spark.{SparkConf, SparkContext}


object wordcount {
  def main(args: Array[String]): Unit = {
    /*
    new version
     */
    /*   val spark = SparkSession.builder()
         .appName("Spark_2.2_wordcount")
         .config("spark.master", "local")
         .getOrCreate()
       val textfile = spark.sparkContext.textFile("wordcount.csv")
       val count = textfile.flatMap(_.split(",")).map(word => (word, 1)).reduceByKey(_ + _)
       //count.saveAsTextFile("wc.out")
       count.foreach(println)*/

    /**
      * old version
      */

    val config = new SparkConf().setAppName("sc app").setMaster("local")
    val sc = new SparkContext(config)
    val textfile = sc.textFile("wordcount.csv")
    //textfile.foreach(println)
    val flatmapFile = textfile.flatMap(_.split(","))
    //flatmapFile.foreach(println)
    val mapFile = flatmapFile.map(word => (word, 1))
    //mapFile.foreach(println)
    val wordcount = mapFile.reduceByKey(_ + _)
    wordcount.sortByKey().foreach(println)


    val unique = mapFile.reduceByKey((v1, v2) => v1) //remove duplicate key
    unique.sortByKey().foreach(println) //sort keys from tuple

    val filtered = wordcount.filter(_._2 <= 1)
    filtered.sortByKey().foreach(println)


  }
}
