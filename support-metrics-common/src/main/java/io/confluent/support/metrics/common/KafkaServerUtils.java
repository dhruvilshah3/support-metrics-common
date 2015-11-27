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
  private static EmbeddedZookeeper zookeeper = null;
  private static Properties serverConfig = null;

  static {
    try {
      Properties props = new Properties();
      props.load(Version.class.getResourceAsStream("/support-metrics-common-default-server.properties"));
      serverConfig = props;
    } catch (IOException e) {
      log.warn("Error while loading default properties:", e.getMessage());
    }
  }

  private static void startZookeeper() {
    zookeeper = new EmbeddedZookeeper();
  }

  private static void stopZookeeper(EmbeddedZookeeper zookeeper) {
    if (zookeeper == null) {
      return;
    }
    zookeeper.shutdown();
  }

  public static KafkaServer startServer() {

    if (zookeeper == null) {
      startZookeeper();
    }
    int brokerId = 0;
    Option<SecurityProtocol> so = Option.apply(SecurityProtocol.PLAINTEXT);
    Properties props = TestUtils.createBrokerConfig(brokerId, "localhost:" + zookeeper.port(), true, false, 0,
        so, Option$.MODULE$.<File>empty(), true, false, 0, false, 0, false, 0);
    KafkaServer server = TestUtils.createServer(KafkaConfig.fromProps(props), SystemTime$.MODULE$);

    // change the server config with zookeeper string
    serverConfig.remove(KafkaConfig$.MODULE$.ZkConnectProp());
    serverConfig.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), "localhost:" + zookeeper.port());
    return server;
  }

  public static void stopServer(KafkaServer server) {
    if (server == null) {
      return;
    }
    server.shutdown();
    CoreUtils.rm(server.config().logDirs());
    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }

  /**
   * Returns a default broker configuration if broker has not started. If broker
   * has started returns the current broker configuration (with updated zookeeper string)
   * @return
   */
  public static Properties getDefaultBrokerConfiguration() {
    return serverConfig;
  }
}
