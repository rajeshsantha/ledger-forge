package com.ledgerforge.pipeline.utils

object RuntimeCalculator {
def calcRuntime[T](operationName: String)(operation: => T): T = {
  val start = System.nanoTime()
  val result = operation
  val end = System.nanoTime()
  val totalMillis = (end - start) / 1000000L
  val minutes = totalMillis / 60000
  val seconds = (totalMillis % 60000) / 1000
  val millis = totalMillis % 1000
  println(s"Elapsed time for $operationName: ${minutes}m ${seconds}s ${millis}ms")
  result
  }
}
