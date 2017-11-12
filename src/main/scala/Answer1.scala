import org.apache.spark.SparkContext

import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
import org.apache.spark.sql.{SQLContext,Row,DataFrame}
import org.apache.spark.sql.functions._




object Answer1 {

  def main(args:Array[String]){
  val sc = new SparkContext("local[4]", "Dataframe creation example")
  val sqlContext = new SQLContext(sc)


    //Load Ratings data
    val ratingsRDD = sc.textFile("ml-1m/ratings.dat")
   val ratingsRowRDD = ratingsRDD.map(record=> Row.fromSeq(record.split("::").map(value => value.toInt)))
    val ratingSchema = StructType(List(StructField("UserID",IntegerType,false),StructField("MovieID",IntegerType,false),StructField("Ratings",IntegerType,false),StructField("Timestamp",IntegerType,false)))
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

    val topTenMovies= ratingsDF.groupBy("MovieID").count().orderBy(desc("count")).limit(10)

    topTenMovies.join(movieDF,"MovieID").select("Title","count").show()

  }
}
