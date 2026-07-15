/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.realtime;

/**
 * Identifies a partition group by the topic (stream config index) it belongs to and its raw, unpadded partition id
 * within that topic. Raw partition ids are only unique within a single topic, so any map keyed by partition id
 * across multiple topics must use this composite key instead to avoid conflating partitions from different topics
 * that happen to share the same raw id.
 */
public record TopicPartitionId(int topicId, int streamPartitionId) {
}
