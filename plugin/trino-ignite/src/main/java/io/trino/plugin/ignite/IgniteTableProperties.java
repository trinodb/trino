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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.ignite.IgniteTableProperties.IgniteTemplateType.PARTITIONED;
import static io.trino.plugin.ignite.IgniteTableProperties.IgniteWriteSyncMode.FULL_SYNC;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IgniteTableProperties
        implements TablePropertiesProvider
{
    private final List<PropertyMetadata<?>> tableProperties;

    private static final int DEFAULT_REPLICATION_NUMBERS = 1;
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";
    public static final String BACKUPS_PROPERTY = "backups";
    public static final String AFFINITY_KEY_PROPERTY = "affinity_key";
    public static final String TEMPLATE_PROPERTY = "template";
    public static final String WRITE_SYNCHRONIZATION_MODE_PROPERTY = "write_synchronization_mode";
    public static final String CACHE_GROUP_PROPERTY = "cache_group";
    public static final String CACHE_NAME_PROPERTY = "cache_name";
    public static final String DATA_REGION_PROPERTY = "data_region";

    public static final Set<String> WITH_PROPERTIES = ImmutableSet.of(
            BACKUPS_PROPERTY,
            AFFINITY_KEY_PROPERTY,
            TEMPLATE_PROPERTY,
            WRITE_SYNCHRONIZATION_MODE_PROPERTY,
            CACHE_GROUP_PROPERTY,
            CACHE_NAME_PROPERTY,
            DATA_REGION_PROPERTY);

    @Inject
    public IgniteTableProperties()
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PRIMARY_KEY_PROPERTY,
                        "The primary keys for the table",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                integerProperty(
                        BACKUPS_PROPERTY,
                        "The backup number for the table",
                        DEFAULT_REPLICATION_NUMBERS,
                        value -> checkArgument(value > 0, "backups should greater than 0"),
                        false),
                stringProperty(
                        AFFINITY_KEY_PROPERTY,
                        "The affinity key in Ignite, must be one of the primary key",
                        null,
                        false),
                enumProperty(
                        TEMPLATE_PROPERTY,
                        "Ignite table template, defaults to PARTITIONED",
                        IgniteTemplateType.class,
                        PARTITIONED,
                        false),
                enumProperty(
                        WRITE_SYNCHRONIZATION_MODE_PROPERTY,
                        "Ignite write synchronization mode, defaults to FULL_SYNC",
                        IgniteWriteSyncMode.class,
                        FULL_SYNC,
                        false),
                stringProperty(
                        CACHE_GROUP_PROPERTY,
                        "Specifies the group name the underlying cache belongs to",
                        null,
                        false),
                stringProperty(
                        CACHE_NAME_PROPERTY,
                        "The name of the underlying cache created by the command.",
                        null,
                        false),
                stringProperty(
                        DATA_REGION_PROPERTY,
                        "name of the data region where table entries should be stored",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static String getAffinityKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (String) tableProperties.get(AFFINITY_KEY_PROPERTY);
    }

    public enum IgniteTemplateType
    {
        REPLICATED,
        PARTITIONED,
    }

    public enum IgniteWriteSyncMode
    {
        PRIMARY_SYNC,
        FULL_SYNC,
        FULL_ASYNC,
    }
}
