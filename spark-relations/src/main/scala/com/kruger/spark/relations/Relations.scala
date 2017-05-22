import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Relations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Matrix multiplication")
    val sc = new SparkContext(conf)

    val relations = sc.textFile(args(0)).filter(!_.isEmpty()).map(r => parseRelation(r).swap)

    relations
      .map(_._1)
      .distinct()
      .saveAsTextFile("output/sum")

    relations
      .reduceByKey((r1, r2) => r1 + r2)
      .filter(r => r._2.contains("A") && r._2.contains("B"))
      .map(_._1)
      .distinct()
      .saveAsTextFile("output/intersection")

    relations
      .reduceByKey((r1, r2) => r1 + r2)
      .filter(r => r._2.contains("A") && !r._2.contains("B"))
      .map(_._1)
      .distinct()
      .saveAsTextFile("output/a_minus_b")

  }

  def parseRelation(s: String) = {
    val split = s.split("\\t", 2)
    (split(0), split(1))
  }
}
