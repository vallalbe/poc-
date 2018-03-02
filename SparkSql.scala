package sparkSql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkSql {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlExample")
      .enableHiveSupport()
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.option("header", "true").csv("student.csv")

    // Displays the ddl of the DataFrame to stdout
    df.printSchema()
    // Displays the content of the DataFrame to stdout
    df.show()


    val gradeUDF = udf { marks: String =>
      val mark = marks.toInt
      if (mark >= 35 && mark <= 50)
        "PASS"
      else if (mark >= 50 && mark <= 75)
        "AVERAGE"
      else if (mark > 75)
        "EXCELLENT"
      else
        "FAIL"
    }

    val updatedDf = df.withColumn("Grade", gradeUDF(df.col("marks")))
    updatedDf.show()

    // Register the DataFrame as a SQL temporary view
    updatedDf.createTempView("student")
    spark.sql("select Grade,count(marks) count from student group by Grade order by count").show()


    val sqlDF = updatedDf.groupBy("marks").count().orderBy("marks")
    sqlDF.show()


    /*
    Using sparkContext
     */
    /*val sparkconf = new SparkConf().setAppName("SparkSqlExample").setMaster("local")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.option("header", "true").csv("student.csv")
    df.show()

    val gradeUDF = udf{marks:String=>
      val mark = marks.toInt
      if (mark >= 35 && mark <= 50)
        "PASS"
      else if (mark >= 50 && mark <= 75)
        "AVERAGE"
      else if (mark > 75)
        "EXCELLENT"
      else
        "FAIL"
    }

    val updatedDf=df.withColumn("Grade", gradeUDF(df.col("marks")))*/

  }

}
