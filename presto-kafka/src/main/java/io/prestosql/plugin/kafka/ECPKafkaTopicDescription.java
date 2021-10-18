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
package io.prestosql.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a row on a Kafka topic. A row contains a message and an optional key. See the documentation for the exact JSON syntax.
 */
public class ECPKafkaTopicDescription
{
    private final String tableName;
    private final String topicPrefix;
    private final Optional<KafkaTopicFieldGroup> key;
    private final Optional<KafkaTopicFieldGroup> message;

    @JsonCreator
    public ECPKafkaTopicDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicPrefix") String topicPrefix,
            @JsonProperty("key") Optional<KafkaTopicFieldGroup> key,
            @JsonProperty("message") Optional<KafkaTopicFieldGroup> message)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix is null");
        this.key = requireNonNull(key, "key is null");
        this.message = requireNonNull(message, "message is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTopicPrefix()
    {
        return topicPrefix;
    }

    @JsonProperty
    public Optional<KafkaTopicFieldGroup> getKey()
    {
        return key;
    }

    @JsonProperty
    public Optional<KafkaTopicFieldGroup> getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("topicPrefix", topicPrefix)
                .add("key", key)
                .add("message", message)
                .toString();
    }
}
