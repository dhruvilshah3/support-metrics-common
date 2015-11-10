package io.confluent.support.metrics.common;

public class TimeUtils {

  /**
   * Returns the current time in seconds since the epoch (aka Unix time).
   */
  public long nowInUnixTime() {
    return System.currentTimeMillis() / 1000;
  }

}