package io.confluent.support.metrics.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UuidTest {

  @Test
  public void generatesValidType4UUID() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String generatedUUID = uuid.getUUID();

    // Then
    assertThat(generatedUUID).matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}");
  }

  @Test
  public void stringRepresentationIsIdenticalToGeneratedUUID() {
    // Given
    Uuid uuid = new Uuid();

    // When/Then
    assertThat(uuid.toString()).isEqualTo(uuid.getUUID());
  }

  @Test
  public void uuidDoesNotChangeBetweenRuns() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String firstUuid = uuid.getUUID();
    String secondUuid = uuid.getUUID();

    // Then
    assertThat(firstUuid).isEqualTo(secondUuid);
  }

}