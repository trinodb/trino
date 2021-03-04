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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Clickhouse connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo (a VARCHAR , b INT) WITH (engine='Log');</pre>
 * </p>
 */
public final class ClickHouseTableProperties
        implements TablePropertiesProvider
{
    public static final String ENGINE_PROPERTY = "engine";
    public static final String DEFAULT_TABLE_ENGINE = "Log";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ClickHouseTableProperties()
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        ENGINE_PROPERTY,
                        "ClickHouse Table Engine, defaults to Log",
                        DEFAULT_TABLE_ENGINE,
                        false));
    }

    public static Optional<String> getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        return Optional.ofNullable(tableProperties.get(ENGINE_PROPERTY)).map(String.class::cast);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
