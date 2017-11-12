
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._




object Answer2 {

  def main(args:Array[String]){
    val sc = new SparkContext("local[4]", "Dataframe creation example")
    val sqlContext = new SQLContext(sc)


    //Load Ratings data
    val ratingsRDD = sc.textFile("ml-1m/ratings.dat")
    val ratingsRowRDD = ratingsRDD.map(record=> Row.fromSeq(record.split("::").map(value => value.toInt)))
    val ratingSchema = StructType(List(StructField("UserID",IntegerType,false),StructField("MovieID",IntegerType,false),StructField("Rating",IntegerType,false),StructField("Timestamp",IntegerType,false)))
    val ratingsDF = sqlContext.createDataFrame(ratingsRowRDD,ratingSchema)
    ratingsDF.printSchema()


    //Load Movie data
    val movieRDD = sc.textFile("ml-1m/movies.dat")
    val movieRowRDD = movieRDD.map(record => {
      val cols =  record.split("::")
      Row.fromSeq(List(cols(0).toInt,cols(1).toString,cols(2).toString))
    })
    val movieSchema = StructType(List(StructField("MovieID",IntegerType,false),StructField("Title",StringType,false),StructField("Geners",StringType,false)))
    val movieDF = sqlContext.createDataFrame(movieRowRDD,movieSchema)



    val top20MoviesByRating =ratingsDF.groupBy("MovieID")
      .agg(avg("Rating").as("AvgRating"),count("UserID").as("NoOfReviewers"))
      .filter("NoOfReviewers>=40")
      .orderBy(desc("AvgRating"))
      .limit(20)


    top20MoviesByRating
      .join(movieDF,"MovieID")
      .select("Title","AvgRating")
      .show()

  }
}
