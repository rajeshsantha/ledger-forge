package com.ledgerforge.pipeline.config

import java.util.Properties
import scala.util.Try

/**
 * Centralized build information extracted from JAR manifest and build metadata.
 * Provides version, application name, build time, and other build-related information.
 */
object BuildInfo {

  private lazy val buildInfoProperties: Properties = {
    val props = new Properties()
    Try {
      val stream = getClass.getResourceAsStream("/META-INF/build-info.properties")
      if (stream != null) {
        try {
          props.load(stream)
        } finally {
          stream.close()
        }
      }
    }
    props
  }

  /**
   * Application name
   */
  val name: String = buildInfoProperties.getProperty("build.name", "LedgerForge")

  /**
   * Application version from Maven POM
   */
  val version: String = buildInfoProperties.getProperty("build.version", "unknown")

  /**
   * Build timestamp
   */
  val buildTime: String = buildInfoProperties.getProperty("build.time", "unknown")

  /**
   * Git commit hash (if available)
   */
  val gitCommit: String = buildInfoProperties.getProperty("build.git.commit", "unknown")

  /**
   * Git branch (if available)
   */
  val gitBranch: String = buildInfoProperties.getProperty("build.git.branch", "unknown")

  /**
   * Full version string including name and version
   */
  def fullVersion: String = s"$name version $version"

  /**
   * Detailed build info string
   */
  def detailedInfo: String =
    s"""$name version $version
       |Built: $buildTime
       |Git: $gitBranch @ ${gitCommit.take(8)}""".stripMargin

  /**
   * Check if we're running from a JAR or in development mode
   */
  def isDevelopmentMode: Boolean = version == "unknown" || version.endsWith("-SNAPSHOT")
}
