/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record KafkaFilteringResult(
        List<PartitionInfo> partitionInfos,
        Map<TopicPartition, Long> partitionBeginOffsets,
        Map<TopicPartition, Long> partitionEndOffsets)
{
    public KafkaFilteringResult
    {
        partitionInfos = ImmutableList.copyOf(requireNonNull(partitionInfos, "partitionInfos is null"));
        partitionBeginOffsets = ImmutableMap.copyOf(requireNonNull(partitionBeginOffsets, "partitionBeginOffsets is null"));
        partitionEndOffsets = ImmutableMap.copyOf(requireNonNull(partitionEndOffsets, "partitionEndOffsets is null"));
    }
}
