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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public final class JdbcJoinPushdownSessionProperties
        implements SessionPropertiesProvider
{
    public static final String JOIN_PUSHDOWN_STRATEGY = "join_pushdown_strategy";
    public static final String JOIN_PUSHDOWN_AUTOMATIC_MAX_TABLE_SIZE = "join_pushdown_automatic_max_table_size";
    public static final String JOIN_PUSHDOWN_AUTOMATIC_MAX_JOIN_TO_TABLES_RATIO = "join_pushdown_automatic_max_join_to_tables_ratio";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JdbcJoinPushdownSessionProperties(JdbcJoinPushdownConfig joinPushdownConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        JOIN_PUSHDOWN_STRATEGY,
                        "Join pushdown strategy",
                        JoinPushdownStrategy.class,
                        joinPushdownConfig.getJoinPushdownStrategy(),
                        false))
                .add(doubleProperty(
                        JOIN_PUSHDOWN_AUTOMATIC_MAX_JOIN_TO_TABLES_RATIO,
                        "If estimated join output size is greater than or equal to ratio * sum of table sizes, then join pushdown will not be performed",
                        joinPushdownConfig.getJoinPushdownAutomaticMaxJoinToTablesRatio(),
                        false))
                .add(dataSizeProperty(
                        JOIN_PUSHDOWN_AUTOMATIC_MAX_TABLE_SIZE,
                        "Maximum table size to be considered for join pushdown",
                        joinPushdownConfig.getJoinPushdownAutomaticMaxTableSize().orElse(null),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static JoinPushdownStrategy getJoinPushdownStrategy(ConnectorSession session)
    {
        return session.getProperty(JOIN_PUSHDOWN_STRATEGY, JoinPushdownStrategy.class);
    }

    public static Optional<DataSize> getJoinPushdownAutomaticMaxTableSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(JOIN_PUSHDOWN_AUTOMATIC_MAX_TABLE_SIZE, DataSize.class));
    }

    public static double getJoinPushdownAutomaticJoinToTablesRatio(ConnectorSession session)
    {
        return session.getProperty(JOIN_PUSHDOWN_AUTOMATIC_MAX_JOIN_TO_TABLES_RATIO, Double.class);
    }
}
