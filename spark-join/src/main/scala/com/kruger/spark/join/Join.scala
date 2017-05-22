import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Relations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Matrix multiplication")
    val sc = new SparkContext(conf)

    val relations = sc
      .textFile(args(0))
      .filter(!_.isEmpty())
      .map(r => parseTuple(r))

    relations
      .reduceByKey((a, b) => a + b)
      .saveAsTextFile("output/join")
  }

  def parseTuple(s: String) = {
    if (s.startsWith("Orders")) {
      val split = s.split("\\t", 4)
      (split(2), "Orders(" + split(1) + "," + split(3) + ")")
    } else if (s.startsWith("Customers")) {
      val split = s.split("\\t", 3)
      (split(1), "Customers(" + split(2) + ")")
    } else {
      throw new IllegalArgumentException("Unknown tuple")
    }
  }
}
