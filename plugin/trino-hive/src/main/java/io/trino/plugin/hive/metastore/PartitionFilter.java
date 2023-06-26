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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionFilter
{
    private final HiveTableName hiveTableName;
    private final List<String> partitionColumnNames;
    private final TupleDomain<String> partitionKeysFilter;

    @JsonCreator
    public PartitionFilter(
            @JsonProperty("hiveTableName") HiveTableName hiveTableName,
            @JsonProperty("partitionColumnNames") List<String> partitionColumnNames,
            @JsonProperty("partitionKeysFilter") TupleDomain<String> partitionKeysFilter)
    {
        this.hiveTableName = requireNonNull(hiveTableName, "hiveTableName is null");
        this.partitionColumnNames = ImmutableList.copyOf(requireNonNull(partitionColumnNames, "partitionColumnNames is null"));
        this.partitionKeysFilter = requireNonNull(partitionKeysFilter, "partitionKeysFilter is null");
    }

    public static PartitionFilter partitionFilter(String databaseName, String tableName, List<String> partitionColumnNames, TupleDomain<String> partitionKeysFilter)
    {
        return new PartitionFilter(hiveTableName(databaseName, tableName), partitionColumnNames, partitionKeysFilter);
    }

    @JsonProperty
    public HiveTableName getHiveTableName()
    {
        return hiveTableName;
    }

    @JsonProperty
    public TupleDomain<String> getPartitionKeysFilter()
    {
        return partitionKeysFilter;
    }

    @JsonProperty
    public List<String> getPartitionColumnNames()
    {
        return partitionColumnNames;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hiveTableName", hiveTableName)
                .add("partitionColumnNames", partitionColumnNames)
                .add("partitionKeysFilter", partitionKeysFilter)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionFilter other = (PartitionFilter) o;
        return Objects.equals(hiveTableName, other.hiveTableName) &&
                Objects.equals(partitionColumnNames, other.partitionColumnNames) &&
                Objects.equals(partitionKeysFilter, other.partitionKeysFilter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hiveTableName, partitionColumnNames, partitionKeysFilter);
    }
}
