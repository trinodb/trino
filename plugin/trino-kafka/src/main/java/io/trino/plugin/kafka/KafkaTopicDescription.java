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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a row on a Kafka topic. A row contains a message and an optional key. See the documentation for the exact JSON syntax.
 */
public record KafkaTopicDescription(
        String tableName,
        Optional<String> schemaName,
        String topicName,
        Optional<KafkaTopicFieldGroup> key,
        Optional<KafkaTopicFieldGroup> message)
{
    public KafkaTopicDescription
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        requireNonNull(topicName, "topicName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(key, "key is null");
        requireNonNull(message, "message is null");
    }
}
