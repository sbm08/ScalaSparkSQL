import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties
object SparkApllication {

  // Creating a Spark Session
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Hello")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir","D:/csv/spark_result")
    .getOrCreate()
  println("************************* Spark Application Started *************************")
  
  // Creating variable for Database connection
  
  val url = "jdbc:mysql://localhost:3306"
  val cpd = "employees.current_dept_emp"
  val dp = "employees.departments"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "MyNewPass")
  Class.forName("com.mysql.jdbc.Driver")
  
  // Creating Dataframes from the Tables
  
  val mySQLDF: DataFrame = spark.read.jdbc(url, cpd , properties)
  val mySQLDF2: DataFrame = spark.read.jdbc(url, dp ,properties)
  println("************************* DataFrame 1 ************************* ")
  mySQLDF.show()
  println("************************* DataFrame 2 ************************* ")
  mySQLDF2.show()
  
  // Inner Joining the Dataframes
  
  val deptDF: DataFrame = mySQLDF.join(mySQLDF2,Seq("dept_no"))
  println("************************* Joined Dataframes ************************* ")
  deptDF.show()
  println("************************* Total Count " + deptDF.count() + " *************************")
  val table = "spark_result.join_df"
  deptDF.write.mode(SaveMode.Overwrite).jdbc(url,table ,properties)
  spark.stop()
  println("************************* Spark Application Stopped ************************* ")
  def main(args: Array[String]): Unit = {
  }
}
