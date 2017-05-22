import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

object TransitiveClosure {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Matrix multiplication")
    val sc = new SparkContext(conf)

    val left = loadMatrix(sc, args(0))
    val right = loadMatrix(sc, args(1))

    val mul = left.multiply(right)

    mul.toCoordinateMatrix().entries.map(matrixEntryToString).saveAsTextFile(args(2))
  }

  def parseMatrixElement(s: String) = {
    val coordValue = s.split("\\t", 2)
    val coord = coordValue(0).split(",")
    val value = coordValue(1)
    MatrixEntry(coord(0).toLong, coord(1).toLong, value.toDouble)
  }

  def loadMatrix(sc: SparkContext, path: String) = {
    val file = sc.textFile(path)
    val rdd = file.map(parseMatrixElement)
    new CoordinateMatrix(rdd).toBlockMatrix()
  }

  def matrixEntryToString(me: MatrixEntry) = {
    me.i + "," + me.j + "\t" + me.value
  }
}
