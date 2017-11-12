
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext




object Answer3 {

  def main(args:Array[String]){
    val sc = new SparkContext("local[4]", "Dataframe creation example")
    val sqlContext = new HiveContext(sc)


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


    //Load User data
    val usersRDD = sc.textFile("ml-1m/users.dat")
    val usersRowRDD = usersRDD.map(record => {
      val cols =  record.split("::")
      Row.fromSeq(List(cols(0).toInt,cols(1).toString,cols(2).toInt,cols(3).toInt,cols(4).toString))
    })
    val usersSchema = StructType(List(StructField("UserID",IntegerType,false),StructField("Gender",StringType,false),StructField("Age",IntegerType,false),
      StructField("Occupation",IntegerType,true),StructField("ZipCode",StringType,false)))
    val usersDF = sqlContext.createDataFrame(usersRowRDD,usersSchema)

    usersDF.printSchema()

   val occupationMap = Map(0->"other",1->"academic/educator",2->"artist",3->"clerical/admin",4->"college/grad student",5->"customer service",
      6->"doctor/health care",7->"executive/managerial",8->"farmer",9->"homemaker",10->"K-12 student",
      11->"lawyer",12->"programmer",13->"retired",14->"sales/marketing",15->"scientist",
      16->"self-employed",17->"technician/engineer",18->"tradesman/craftsman",19->"unemployed",20->"writer")

    def occupationConvert(occupationInt: Integer): String ={occupationMap(occupationInt)}
    val occupationConvertUDF = udf(occupationConvert _)

    def ageBucket(age:Integer):String = age match {
      case x if x<18 => "Under 18"
      case x if x<25 => "18-24"
      case x if x<35 => "25-34"
      case x if x<45 => "35-44"
      case x if x<50 => "45-49"
      case x if x<56 =>"50-55"
      case _ => "56+"

    }
    val ageBucketUDF = udf(ageBucket _)


    val  usersUpdatedDF= usersDF.withColumn("OccupationString",occupationConvertUDF(usersDF("Occupation"))).withColumn("AgeBucket",ageBucketUDF(usersDF("Age")))

    usersUpdatedDF.printSchema()



    val movieGenerDF = movieDF.withColumn("Gener",explode(split(movieDF("Geners"), "[|]")))

    val movieGenerRatingDF = usersUpdatedDF.join(ratingsDF,"UserID").join(movieGenerDF,"MovieID").groupBy("OccupationString","AgeBucket","Gener").agg(avg("Ratings").as("AvgRating"))

    //movieGenerDF.select("Gener").distinct().show

   val groupWindow = Window.partitionBy("OccupationString","AgeBucket","Gener").orderBy(col("AvgRating").desc)

    movieGenerRatingDF.withColumn("Rank",dense_rank().over(groupWindow)).filter("Rank<=5").show(100)



  }
}
