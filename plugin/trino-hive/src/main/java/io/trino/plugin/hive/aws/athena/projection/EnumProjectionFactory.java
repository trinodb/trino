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

import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyRequiredValue;

public class EnumProjectionFactory
        implements ProjectionFactory
{
    @Override
    public boolean isSupportedColumnType(Type columnType)
    {
        return columnType instanceof VarcharType;
    }

    @Override
    public Projection create(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        return new EnumProjection(
                columnName,
                getProjectionPropertyRequiredValue(
                        columnName,
                        columnProperties,
                        COLUMN_PROJECTION_VALUES,
                        value -> ((List<?>) value).stream()
                                .map(String::valueOf)
                                .collect(toImmutableList())));
    }
}
