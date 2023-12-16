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

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class KafkaSessionProperties
        implements SessionPropertiesProvider
{
    /**
     * The session property name for enabling or disabling timestamp upper bound push down for topics using "CreateTime" mode.
     * The upper bound predicate is pushed down only for topics using "LogAppendTime" mode.
     * By default, this property is set to false.
     * To enable timestamp upper bound push down, set this property to true.
     *
     * @see KafkaSessionProperties#isTimestampUpperBoundPushdownEnabled(ConnectorSession)
     */
    private static final String TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED = "timestamp_upper_bound_force_push_down_enabled";
    /**
     * Represents the session property name for enabling or disabling predicate force push down.
     *
     * By default, predicate push down is enabled. This means that predicate push down is attempted for all queries.
     *
     * Predicate push down can be disabled via the "kafka.predicate_force_push_down_enabled" config property
     * or the "predicate_force_push_down_enabled" session property.
     *
     * @see KafkaSessionProperties#isPredicatePushDownEnabled(ConnectorSession)
     */
    private static final String PREDICATE_FORCE_PUSH_DOWN_ENABLED = "predicate_force_push_down_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties(KafkaConfig kafkaConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED,
                        "Enable or disable timestamp upper bound push down for topic createTime mode",
                        kafkaConfig.isTimestampUpperBoundPushDownEnabled(),
                        false),
                booleanProperty(
                        PREDICATE_FORCE_PUSH_DOWN_ENABLED,
                        "Enable or disable timestamp upper bound push down for topic createTime mode",
                        kafkaConfig.isPredicateForcePushDownEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
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

    /**
     * By default, it supports predicate push-down.
     * Push-down can be disabled via ``kafka.predicate-force-push-down-enabled`` config prop
     * or ``kafka.predicate_force_push_down_enabled`` session prop.
     */
    public static boolean isPredicatePushDownEnabled(ConnectorSession session)
    {
        return session.getProperty(PREDICATE_FORCE_PUSH_DOWN_ENABLED, Boolean.class);
    }
}
