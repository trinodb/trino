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
package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class StarrocksTableProperties
{
    // TODO:add more properties
    public static final String PROPERTIES_PARTITIONED_BY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    StarrocksTableProperties()
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PROPERTIES_PARTITIONED_BY,
                        "columns to be the partition key. it's optional for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value.isEmpty() ? ImmutableList.of() : value));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
