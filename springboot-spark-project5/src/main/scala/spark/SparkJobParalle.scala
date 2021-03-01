package spark

import org.apache.spark.SparkConf

/**
 * 测试 调整spark的并行度
 * */
object SparkJobParalle {
  def main(args: Array[String]): Unit = {
    /**
     *  并行度：spark.default.parallelism 是并行度，一般是cpu和数的2-3备
     *  setMaster()
     * （1）local 模式：本地单线程运行；
     * （2）local[k]模式：本地K个线程运行；
     * （3）local[*]模式：用本地尽可能多的线程运行。
     * */
    val conf: SparkConf = new SparkConf().setAppName("SparkJobParalle")
                                         .setMaster("local[*]").set("spark.default.parallelism", "32")
  }
}
