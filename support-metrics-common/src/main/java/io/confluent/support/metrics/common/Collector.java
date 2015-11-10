package io.confluent.support.metrics.common;

import org.apache.avro.generic.GenericContainer;

public interface Collector {

  /**
   * Collects metrics from a Kafka broker.
   * @return An Avro record that contains the collected metrics.
   */
  GenericContainer collectMetrics();

}