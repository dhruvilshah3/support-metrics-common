/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.support.metrics.common;

import java.util.Map;

public interface Filter<K> {

  /**
   * Apply the filter, thereby removing elements from it where appropriate.  Modifies the input
   * argument in-place.
   *
   * @param m The input map, which is modified in-place by the filter.  If this is not what you
   *          want, then create a copy of the data structure prior to applying the filter.
   */
  <V> void apply(Map<K, V> m);

}