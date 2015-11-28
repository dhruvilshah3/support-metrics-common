package io.confluent.support.metrics.common;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import kafka.Kafka;
import scala.Option;
import scala.Option$;
import scala.sys.Prop;

/**
 * Helper class for starting an embedded zookeeper and broker.
 * This class is NOT thread safe
 */
public class KafkaServerUtils {
  private static final Logger log = LoggerFactory.getLogger(KafkaServerUtils.class);
  private EmbeddedZookeeper zookeeper = null;
  private static Properties defaultServerConfig = null;
  private KafkaServer server = null;
  private boolean serverStarted = false;

  static {
    try {
      Properties props = new Properties();
      props.load(Version.class.getResourceAsStream("/support-metrics-common-default-server.properties"));
      defaultServerConfig = props;
    } catch (IOException e) {
      log.warn("Error while loading default properties:", e.getMessage());
    }
  }

  private void startZookeeper() {
    zookeeper = new EmbeddedZookeeper();
  }

  private void stopZookeeper() {
    if (zookeeper == null) {
      return;
    }
    zookeeper.shutdown();
    zookeeper = null;
  }

  public String getZookeeperConnection() {
    if (zookeeper != null) {
      return "localhost:" + zookeeper.port();
    } else {
      return null;
    }
  }

  public KafkaServer startServer() {
    if (serverStarted) {
      log.error("Server already started");
      return null;
    }

    if (zookeeper == null) {
      startZookeeper();
    }
    int brokerId = 0;
    Option<SecurityProtocol> so = Option.apply(SecurityProtocol.PLAINTEXT);
    Properties props = TestUtils.createBrokerConfig(brokerId, "localhost:" + zookeeper.port(), true, false, 0,
        so, Option$.MODULE$.<File>empty(), true, false, 0, false, 0, false, 0);
    server = TestUtils.createServer(KafkaConfig.fromProps(props), SystemTime$.MODULE$);

    return server;
  }

  public void stopServer() {
    if (server == null) {
      return;
    }
    server.shutdown();
    CoreUtils.rm(server.config().logDirs());
    server = null;
    if (zookeeper != null) {
      stopZookeeper();
    }
  }

  /**
   * Returns a newly allocated default broker configuration if broker has not started.
   * @return
   */
  public Properties getDefaultBrokerConfiguration() {
    return (Properties)defaultServerConfig.clone();
  }
}
