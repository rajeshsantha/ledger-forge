package com.ledgerforge.pipeline.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val config: Config = ConfigFactory.load()
}
