import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TestApp extends {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Test")
      .getOrCreate()

    import spark.implicits._

    val documents: DataFrame = spark.table("test.documents").select("document_type", "created_at", "id")
    //|-- document_type: byte (nullable = true)
    //|-- created_at: string (nullable = true)
    //|-- id: integer (nullable = true)
    //+-------------+---------------------+---+
    //|document_type|created_at           |id |
    //+-------------+---------------------+---+
    //|1            |2006-10-24 03:00:38.0|95 |
    //|.............|.....................|...|
    //|.............|.....................|...|
    //|2            |2006-11-06 10:28:41.0|150|
    //+-------------+---------------------+---+
    //documents.count() = 100000000
    val types: DataFrame = Seq(1, 2, 3).toDF("doc_type")

    val documentsCreatedBetweenJanAndNov2017: DataFrame = documents
      .join(types, 'document_type === 'doc_type)
      .where(substring('created_at, 1, 4) === 2017)
      .groupBy('document_type).agg(max('created_at) as "max_created")
      .where('max_created < "2017-12-01 00:00:00.000")
    /*
    documentsCreatedBetweenJanAndNov2017.explain
    == Physical Plan ==
      *(3) Filter (isnotnull(max_created#111) && (max_created#111 < 2017-12-01 00:00:00.000))
    +- SortAggregate(key=[document_type#18], functions=[max(created_at#3)], output=[document_type#18, max_created#111])
    +- *(2) Sort [document_type#18 ASC NULLS FIRST], false, 0
    +- Exchange hashpartitioning(document_type#18, 200)
    +- SortAggregate(key=[document_type#18], functions=[partial_max(created_at#3)], output=[document_type#18, max#120])
    +- *(1) Sort [document_type#18 ASC NULLS FIRST], false, 0
    +- *(1) Project [document_type#18, created_at#3]
    +- *(1) BroadcastHashJoin [cast(document_type#18 as int)], [doc_type#92], Inner, BuildRight
    :- *(1) Project [document_type#18, created_at#3]
    :  +- *(1) Filter ((isnotnull(created_at#3) && (cast(substring(created_at#3, 1, 4) as int) = 2017)) && isnotnull(document_type#18))
    :     +- *(1) FileScan parquet test.documents[created_at#3,document_type#18] Batched: trueб Format: Parquet,
    PushedFilters: [IsNotNull(created_at), IsNotNull(document_type)]
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
    +- LocalTableScan [doc_type#92]
  */
       
    val distinctTypeBetweenNovAndDec2017 = documentsCreatedBetweenJanAndNov2017.select('document_type).distinct.collect.length
    println(s"Storing $distinctTypeBetweenNovAndDec2017 documents type")

    documentsCreatedBetweenJanAndNov2017.write.parquet("some parquet path")
    
  }
}

