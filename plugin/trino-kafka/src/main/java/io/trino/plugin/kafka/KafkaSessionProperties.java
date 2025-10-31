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
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

public final class KafkaSessionProperties
        implements SessionPropertiesProvider
{
    public static final String KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES = "kafka_topic_partition_offset_overrides";
    private static final String TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED = "timestamp_upper_bound_force_push_down_enabled";
    private static final String NONE = "";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties(KafkaConfig kafkaConfig)
    {
        sessionProperties = ImmutableList.of(PropertyMetadata.booleanProperty(
                        TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED,
                        "Enable or disable timestamp upper bound push down for topic createTime mode",
                        kafkaConfig.isTimestampUpperBoundPushDownEnabled(), false),
                PropertyMetadata.stringProperty(
                        KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES,
                        "Set custom offsets when reading particular kafka topic partitions. Example config: 'topicName-0=0-10,topicName-1=5-20'. Trino will read topicName partition 0 from offsets 0 to 10 (exclusive). If not specified, Trino will fall back to the current partition of a kafka topic.",
                        NONE, false));
    }

    /**
     * If predicate specifies lower bound on _timestamp column (_timestamp > XXXX), it is always pushed down.
     * The upper bound predicate is pushed down only for topics using ``LogAppendTime`` mode.
     * For topics using ``CreateTime`` mode, upper bound push down must be explicitly
     * allowed via ``kafka.timestamp-upper-bound-force-push-down-enabled`` config property
     * or ``timestamp_upper_bound_force_push_down_enabled`` session property.
     */
    public static boolean isTimestampUpperBoundPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED, Boolean.class);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
