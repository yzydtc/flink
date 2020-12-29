package demo

import org.apache.flink.api.scala._

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    //配置环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements("Who's there" +
      " I think I hear them. Stand, ho! Who's there?")
    val counts = text.flatMap(str=>str.split("\\W+")).map((_,1)).groupBy(0).sum(1)
    counts.print()
  }
}
