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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.stringProperty;

public class BigQueryTableProperties
{
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    public BigQueryTableProperties()
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        PARTITIONED_BY_PROPERTY,
                        "Partition column",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getPartitionedBy(TableDefinition tableDefinition)
    {
        if (tableDefinition instanceof StandardTableDefinition) {
            StandardTableDefinition standardTableDefinition = (StandardTableDefinition) tableDefinition;

            RangePartitioning rangePartition = standardTableDefinition.getRangePartitioning();
            if (rangePartition != null) {
                return Optional.of(rangePartition.getField());
            }

            TimePartitioning timePartition = standardTableDefinition.getTimePartitioning();
            if (timePartition != null) {
                return Optional.ofNullable(timePartition.getField());
            }
        }
        return Optional.empty();
    }
}
