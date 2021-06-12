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
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IgniteTableProperties
        implements TablePropertiesProvider
{
    private final List<PropertyMetadata<?>> tableProperties;

    private static final String PRIMARY_KEY_PROPERTY = "primary_key";
    private static final String BACK_UPS_PROPERTY = "backups";
    private static final String AFFINITY_KEY_PROPERTY = "affinity_key";
    private static final String TEMPLATE = "template";

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
                        BACK_UPS_PROPERTY,
                        "the backup number for the table",
                        1,
                        value -> checkArgument(value > 0, "backups should greater than 0"),
                        false),
                stringProperty(
                        AFFINITY_KEY_PROPERTY,
                        "An expression for sampling. it's optional for table MergeTree engine family",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static IgniteTemplateType getTemplate(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (IgniteTemplateType) tableProperties.get(TEMPLATE);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static Integer getBackups(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (Integer) tableProperties.get(BACK_UPS_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static String getAffinityKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (String) tableProperties.get(AFFINITY_KEY_PROPERTY);
    }
}
