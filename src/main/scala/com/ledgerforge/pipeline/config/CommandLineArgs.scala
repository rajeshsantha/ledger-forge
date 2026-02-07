package com.ledgerforge.pipeline.config

case class CommandLineArgs(env: String = "dev", inputOverride: Option[String] = None)
