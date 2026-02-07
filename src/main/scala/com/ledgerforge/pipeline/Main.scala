package com.ledgerforge.pipeline

import com.ledgerforge.pipeline.config.ArgParser
import com.ledgerforge.pipeline.core.JobRunner

object Main {
  def main(args: Array[String]): Unit = {
    ArgParser.parseArgs(args) match {
      case Some(cmdArgs) => new JobRunner(cmdArgs).run()
      case None =>
        System.err.println("Invalid arguments.")
        System.exit(1)
    }
  }
}

