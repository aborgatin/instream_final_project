package com.github.aborgatin.stream.finalproject.spark

import org.slf4j.LoggerFactory

trait Loggable {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  protected def info(msg: String): Unit = {
    logger.info(msg)
  }

  protected def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  protected def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  protected def warn(msg: String, e: Throwable): Unit = {
    logger.warn(msg, e)
  }

  protected def error(msg: String): Unit = {
    logger.error(msg)
  }

  protected def error(msg: String, e: Throwable): Unit = {
    logger.error(msg, e)
  }

}
