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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.aws.athena.projection.ProjectionType;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_UNIT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_TYPE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class HiveColumnProperties
{
    private final List<PropertyMetadata<?>> columnProperties;

    public HiveColumnProperties()
    {
        columnProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        COLUMN_PROJECTION_TYPE,
                        "Type of partition projection",
                        ProjectionType.class,
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        COLUMN_PROJECTION_VALUES,
                        "Enum column projection values",
                        new ArrayType(VARCHAR),
                        List.class,
                        null,
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .collect(toImmutableList()),
                        value -> value))
                .add(new PropertyMetadata<>(
                        COLUMN_PROJECTION_RANGE,
                        "Column projection range, applicable for date and integer projection type",
                        new ArrayType(VARCHAR),
                        List.class,
                        null,
                        false,
                        value -> ImmutableList.copyOf((List<?>) value),
                        value -> value))
                .add(integerProperty(
                        COLUMN_PROJECTION_INTERVAL,
                        "Integer column projection range interval",
                        null,
                        false))
                .add(enumProperty(
                        COLUMN_PROJECTION_INTERVAL_UNIT,
                        "Date column projection range interval unit",
                        ChronoUnit.class,
                        null,
                        false))
                .add(stringProperty(
                        COLUMN_PROJECTION_FORMAT,
                        "Date column projection format",
                        null,
                        false))
                .add(integerProperty(
                        COLUMN_PROJECTION_DIGITS,
                        "Number of digits to be used with integer column projection",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }
}
