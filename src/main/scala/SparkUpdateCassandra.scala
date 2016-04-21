/* SparkUpdateCassandra.scala */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object SparkUpdateCassandra {
  def main(args: Array[String]) {

    val SparkMasterHost = "local[*]"
    val CassandraHost = "127.0.0.1"

    val conf = new SparkConf(true)
        .setAppName(getClass.getSimpleName)
        .setMaster(SparkMasterHost)
        .set("spark.cassandra.connection.host", CassandraHost)
        .set("spark.cleaner.ttl", "3600")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("\nCreate keyspace 'test', table 'name_counter' and insert entries:")

    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS test.name_counter (name TEXT, surname TEXT, count COUNTER, PRIMARY KEY(name, surname))")
      session.execute("TRUNCATE test.name_counter")
      session.execute("UPDATE test.name_counter SET count=count+100  WHERE name='John'    AND surname='Smith' ")
      session.execute("UPDATE test.name_counter SET count=count+1000 WHERE name='Zhang'   AND surname='Wei'   ")
      session.execute("UPDATE test.name_counter SET count=count+10   WHERE name='Angelos' AND surname='Papas' ")
    }

    val nc = sqlContext.read.format("org.apache.spark.sql.cassandra")
                    .options(Map("keyspace" -> "test", "table" -> "name_counter"))
                    .load()
    nc.show()

    println("\nUpdate table with more counts:")

    val updateRdd = sc.parallelize(Seq(Row("John",    "Smith", 1L),
                                       Row("Zhang",   "Wei",   2L),
                                       Row("Angelos", "Papas", 3L)))
    val tblStruct = new StructType(
        Array(StructField("name",    StringType, nullable = false),
              StructField("surname", StringType, nullable = false),
              StructField("count",   LongType,   nullable = false)))
    val updateDf  = sqlContext.createDataFrame(updateRdd, tblStruct)


    updateDf.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "test", "table" -> "name_counter"))
        .mode("append")
        .save()


    nc.show()

    sc.stop()
  }
}
