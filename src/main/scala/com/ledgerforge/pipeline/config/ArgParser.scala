package com.ledgerforge.pipeline.config

object ArgParser {
  import scopt.OParser

  private val builder = OParser.builder[CommandLineArgs]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("MyBankETL"),
      note("MyBankETL version 1.0"),
      opt[String]('e', "env").action((x, c) => c.copy(env = x)).text("Environment to run in (dev, test, prod)"),
      opt[String]('i', "inputOverride").action((x, c) => c.copy(inputOverride = Some(x))).text("Override input base path (optional)")
    )
  }

  def parseArgs(args: Array[String]): Option[CommandLineArgs] = {
    OParser.parse(parser, args, CommandLineArgs())
  }
}
