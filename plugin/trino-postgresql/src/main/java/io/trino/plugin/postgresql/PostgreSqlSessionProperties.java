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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class PostgreSqlSessionProperties
        implements SessionPropertiesProvider
{
    public static final String ARRAY_MAPPING = "array_mapping";
    public static final String ENABLE_STRING_PUSHDOWN_WITH_COLLATE = "enable_string_pushdown_with_collate";
    public static final String AS_OF_SYSTEM_TIME = "as_of_system_time";
    public static final String AUTO_COMMIT = "auto_commit";
    public static final String FETCH_SIZE = "fetch_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PostgreSqlSessionProperties(PostgreSqlConfig postgreSqlConfig)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(
                        ARRAY_MAPPING,
                        "Handling of PostgreSql arrays",
                        ArrayMapping.class,
                        postgreSqlConfig.getArrayMapping(),
                        false),
                booleanProperty(
                        ENABLE_STRING_PUSHDOWN_WITH_COLLATE,
                        "Enable string pushdown with collate (experimental)",
                        postgreSqlConfig.isEnableStringPushdownWithCollate(),
                        false),
                stringProperty(AS_OF_SYSTEM_TIME, "System time for crdb session", postgreSqlConfig.getAsOfSystemTime().orElse(null), false),
                integerProperty(FETCH_SIZE, "Fetch size for jdbc cursors", postgreSqlConfig.getFetchSize(), false),
                booleanProperty(AUTO_COMMIT, "Auto commit for jdbc connections", postgreSqlConfig.isAutoCommit(), false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static ArrayMapping getArrayMapping(ConnectorSession session)
    {
        return session.getProperty(ARRAY_MAPPING, ArrayMapping.class);
    }

    public static boolean isEnableStringPushdownWithCollate(ConnectorSession session)
    {
        return session.getProperty(ENABLE_STRING_PUSHDOWN_WITH_COLLATE, Boolean.class);
    }

    public static Optional<String> getAsOfSystemTime(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(AS_OF_SYSTEM_TIME, String.class));
    }

    public static boolean getAutoCommit(ConnectorSession session)
    {
        return session.getProperty(AUTO_COMMIT, Boolean.class);
    }

    public static int getFetchSize(ConnectorSession session)
    {
        return session.getProperty(FETCH_SIZE, Integer.class);
    }
}
