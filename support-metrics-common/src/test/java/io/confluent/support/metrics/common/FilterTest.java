package io.confluent.support.metrics.common;

import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FilterTest {

  @Test
  public void doesNotAcceptNullInput() {
    // Given
    Properties nullProperties = null;
    Filter f = new Filter();

    // When/Then
    try {
      f.apply(nullProperties);
      fail("IllegalArgumentException expected because input is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void emptyInputResultsInEmptyOutput() {
    // Given
    Properties emptyProperties = new Properties();
    Filter f = new Filter();

    // When
    Properties filtered = f.apply(emptyProperties);

    // Then
    assertThat(filtered).hasSameSizeAs(emptyProperties);
  }

  @Test
  public void filtersNothingByDefault() {
    // Given
    Properties anyProperties = System.getProperties();
    Filter f = new Filter();

    // When
    Properties filtered = f.apply(anyProperties);

    // Then
    assertThat(filtered).isEqualTo(anyProperties);
    assertThat(filtered).hasSameSizeAs(anyProperties);
  }

  @Test
  public void filtersMatchingKey() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    Set<String> removeOneKey = new HashSet<>();
    removeOneKey.add("one");
    Filter f = new Filter(removeOneKey);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertThat(filtered).hasSize(properties.size() - 1);
    assertThat(filtered).doesNotContainKey("one");
    assertThat(filtered).containsKey("two");
    assertThat(filtered.get("two")).isSameAs(properties.get("two"));
  }

  @Test
  public void filtersMatchingKeys() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    properties.put("three", 3);
    properties.put("four", 4);
    properties.put("five", 5);
    Set<String> removeAllKeys = new HashSet<>();
    for (Object key : properties.keySet()) {
      removeAllKeys.add(key.toString());
    }
    Filter f = new Filter(removeAllKeys);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertThat(filtered).hasSize(0);
  }

  @Test
  public void doesNotFilterMismatchingKeys() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    Set<String> keysToRemove = new HashSet<>();
    keysToRemove.add("three");
    Filter f = new Filter(keysToRemove);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertThat(filtered).hasSameSizeAs(properties);
    assertThat(filtered).containsKeys("one", "two");
  }

}