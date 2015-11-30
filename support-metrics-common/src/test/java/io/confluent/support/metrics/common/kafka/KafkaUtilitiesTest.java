package io.confluent.support.metrics.common.kafka;

import org.junit.Test;

import java.util.Random;

import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class KafkaUtilitiesTest {

  private static final ZkUtils mockZkUtils = mock(ZkUtils.class);
  private static final String anyTopic = "valueNotRelevant";
  private static final int anyPartitions = 1;
  private static final int anyReplication = 1;
  private static final long anyRetentionMs = 1000L;
  private static final long oneYearRetention = 365 * 24 * 60 * 60 * 1000L;
  private static final String[] exampleTopics = {"__confluent.support.metrics", "anyTopic", "basketball"};

  @Test
  public void createTopicThrowsIAEWhenZkUtilsIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.createTopicIfMissing(null, anyTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because zkUtils is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.createTopicIfMissing(mockZkUtils, nullTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.createTopicIfMissing(mockZkUtils, emptyTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.createTopicIfMissing(mockZkUtils, anyTopic, zeroPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.createTopicIfMissing(mockZkUtils, anyTopic, anyPartitions, zeroReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenRetentionMsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    long zeroRetentionMs = 0;

    // When/Then
    try {
      kUtil.createTopicIfMissing(mockZkUtils, anyTopic, anyPartitions, anyReplication, zeroRetentionMs);
      fail("IllegalArgumentException expected because retention.ms is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }


  @Test
  public void verifySupportTopicThrowsIAEWhenZkUtilsIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.verifySupportTopic(null, anyTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because zkUtils is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }


  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkUtils, nullTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkUtils, emptyTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkUtils, anyTopic, zeroPartitions, anyReplication);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkUtils, anyTopic, anyPartitions, zeroReplication);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void underreplicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);

    // When/Then
    for (String topic : exampleTopics) {
      assertThat(kUtil.createTopicIfMissing(broker.zkUtils(), topic, partitions, replication, oneYearRetention)).isTrue();
      // Only one broker is up, so the actual number of replicas will be only 1.
      assertThat(kUtil.verifySupportTopic(broker.zkUtils(), topic, partitions, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Less);
    }
    assertThat(kUtil.getNumTopics(broker.zkUtils())).isEqualTo(exampleTopics.length);
    cluster.stopCluster();
  }


  @Test
  public void underreplicatedTopicsCanBeRecreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);

    // When/Then
    for (String topic : exampleTopics) {
      assertThat(kUtil.createTopicIfMissing(broker.zkUtils(), topic, partitions, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.createTopicIfMissing(broker.zkUtils(), topic, partitions, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.verifySupportTopic(broker.zkUtils(), topic, partitions, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Less);
    }
    assertThat(kUtil.getNumTopics(broker.zkUtils())).isEqualTo(exampleTopics.length);

    // Cleanup
    cluster.stopCluster();
  }

  @Test
  public void createTopicFailsWhenThereAreNoLiveBrokers() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    // Provide us with a realistic but, once the cluster is stopped, defunct instance of zkutils.
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    cluster.startCluster(1);
    KafkaServer broker = cluster.getBroker(0);
    ZkUtils defunctZkUtils = broker.zkUtils();
    cluster.stopCluster();

    // When/Then
    assertThat(kUtil.createTopicIfMissing(defunctZkUtils, anyTopic, anyPartitions, anyReplication, anyRetentionMs)).isFalse();
  }


  @Test
  public void replicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    ZkUtils zkUtils = broker.zkUtils();
    Random random = new Random();
    int replication = numBrokers;

    // When/Then
    for (String topic : exampleTopics) {
      int morePartitionsThanBrokers = random.nextInt(10) + numBrokers + 1;
      assertThat(kUtil.createTopicIfMissing(zkUtils, topic, morePartitionsThanBrokers, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.verifySupportTopic(zkUtils, topic, morePartitionsThanBrokers, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Exactly);
    }

    // Cleanup
    cluster.stopCluster();
  }

}