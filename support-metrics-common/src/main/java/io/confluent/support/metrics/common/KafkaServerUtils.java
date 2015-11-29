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
  public static final int MAX_SERVERS = 10;
  private static final Logger log = LoggerFactory.getLogger(KafkaServerUtils.class);
  private EmbeddedZookeeper zookeeper = null;
  private static Properties defaultServerConfig = null;
  private KafkaServer[] servers = new KafkaServer[MAX_SERVERS];
  private boolean[] serversStarted = new boolean[MAX_SERVERS];

  public KafkaServerUtils() {
    for (int i = 0; i < MAX_SERVERS; i++) {
      serversStarted[i] = false;
    }
  }

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

  /**
   * Starts a specific server with an ID from 0 to MAX_SERVERS-1
   * @param serverId
   * @return
   */
  public KafkaServer startServer(int serverId) {
    if (serversStarted[serverId]) {
      log.error("Server already started");
      return null;
    }

    if (zookeeper == null) {
      startZookeeper();
    }
    Option<SecurityProtocol> so = Option.apply(SecurityProtocol.PLAINTEXT);
    Properties props = TestUtils.createBrokerConfig(serverId, "localhost:" + zookeeper.port(), true, false, 0,
        so, Option$.MODULE$.<File>empty(), true, false, 0, false, 0, false, 0);
    servers[serverId] = TestUtils.createServer(KafkaConfig.fromProps(props), SystemTime$.MODULE$);
    serversStarted[serverId] = true;
    return servers[serverId];
  }

  /**
   * Starts the default server (0)
   * @return
   */
  public KafkaServer startServer() {
    return startServer(0);
  }

  public void stopServer(int serverId) {
    boolean stopZookeeper = true;
    if (servers[serverId] == null) {
      return;
    }
    servers[serverId].shutdown();
    CoreUtils.rm(servers[serverId].config().logDirs());
    servers[serverId] = null;
    serversStarted[serverId] = false;

    // check if all servers have stopped
    for (int i = 0; i < MAX_SERVERS; i++) {
      if (serversStarted[i] == true) {
        stopZookeeper = false;
        break;
      }
    }
    if (zookeeper != null && stopZookeeper == true) {
      stopZookeeper();
    }
  }

  /**
   * Stops the default server (0)
   */
  public void stopServer() {
    stopServer(0);
  }

  /**
   * Returns a newly allocated default broker configuration if broker has not started.
   * @return
   */
  public Properties getDefaultBrokerConfiguration() {
    return (Properties)defaultServerConfig.clone();
  }
}
