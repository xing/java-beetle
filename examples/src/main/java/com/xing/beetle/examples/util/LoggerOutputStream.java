package com.xing.beetle.examples.util;

import java.io.IOException;
import java.io.OutputStream;
import org.slf4j.Logger;

public class LoggerOutputStream extends OutputStream {

  private StringBuffer buffer;
  private final Logger logger;
  private final Level level;

  public enum Level {
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE
  }

  public LoggerOutputStream(Logger logger) {
    this(logger, Level.INFO);
  }

  public LoggerOutputStream(Logger logger, Level level) {
    this.logger = logger;
    this.level = level;
    buffer = new StringBuffer();
  }

  @Override
  public void write(int b) throws IOException {
    if ('\n' == b) {
      flush();
    } else {
      buffer.append((char) b);
    }
  }

  @Override
  public void flush() throws IOException {
    switch (level) {
      case TRACE:
        logger.trace(buffer.toString());
        break;
      case DEBUG:
        logger.debug(buffer.toString());
        break;
      case INFO:
        logger.info(buffer.toString());
        break;
      case WARN:
        logger.warn(buffer.toString());
        break;
      case ERROR:
        logger.error(buffer.toString());
        break;
    }
    buffer = new StringBuffer();
  }
}
