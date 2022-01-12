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
package io.trino.plugin.hive.aws.athena.projection;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyRequiredValue;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyValue;
import static io.trino.plugin.hive.aws.athena.projection.Projection.invalidProjectionException;
import static java.lang.String.format;

public class IntegerProjectionFactory
        implements ProjectionFactory
{
    @Override
    public boolean isSupportedColumnType(Type columnType)
    {
        return columnType instanceof VarcharType
                || columnType instanceof IntegerType
                || columnType instanceof BigintType;
    }

    @Override
    public Projection create(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        List<Integer> range = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_RANGE,
                value -> ((List<?>) value).stream()
                        .map(element -> Integer.valueOf((String) element))
                        .collect(toImmutableList()));
        if (range.size() != 2) {
            invalidProjectionException(
                    columnName,
                    format("Property: '%s' needs to be list of 2 integers", COLUMN_PROJECTION_RANGE));
        }
        return new IntegerProjection(
                columnName,
                range.get(0),
                range.get(1),
                getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_INTERVAL, Integer.class::cast).orElse(1),
                getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_DIGITS, Integer.class::cast));
    }
}
