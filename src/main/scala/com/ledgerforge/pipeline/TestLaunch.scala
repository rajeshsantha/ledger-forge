
package com.ledgerforge.pipeline

object TestLaunch {
  def main(args: Array[String]): Unit = {
    val keys = sys.env.keys.toSeq.sorted

    keys.foreach { k =>
      val v = sys.env.get(k)
      println(s"$k=${v.getOrElse("<not set>")}")
    }
    /*
    val keys = Seq(
      "DB_USER", "DB_PASSWORD")
*/

  }
  }
