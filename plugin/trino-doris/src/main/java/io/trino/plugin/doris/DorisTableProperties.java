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
package io.trino.plugin.doris;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DorisTableProperties
        implements TablePropertiesProvider
{
    private final List<PropertyMetadata<?>> tableProperties;

    public static final String ENGINE_PROPERTY = "engine";
    public static final String DISTRIBUTION_DESC = "distribution_desc";
    public static final String REPLICATION_NUM = "replication_num";

    @Inject
    public DorisTableProperties(DorisConfig starRocksConfig)
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        ENGINE_PROPERTY,
                        "Doris Table Engine, defaults to OLAP",
                        DorisEngineType.class,
                        DorisEngineType.OLAP,
                        false),
                new PropertyMetadata<>(
                        DISTRIBUTION_DESC,
                        "The data distribution keys for the data",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                integerProperty(
                        REPLICATION_NUM,
                        "Value of Doris OLAP table data replication number",
                        starRocksConfig.getOlapTableDefaultReplicationNumber(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static DorisEngineType getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (DorisEngineType) tableProperties.get(ENGINE_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getDistributionDesc(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(DISTRIBUTION_DESC);
    }

    public static int getReplicationNum(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (int) tableProperties.get(REPLICATION_NUM);
    }
}
