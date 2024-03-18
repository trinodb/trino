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
package io.trino.plugin.varada.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HivePartition;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class TestingConnectorTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Map<String, String>> tableParameters;
    private final List<TestingConnectorColumnHandle> partitionColumns;
    private final List<TestingConnectorColumnHandle> dataColumns;
    private final Optional<List<HivePartition>> partitions;
    private final TupleDomain<TestingConnectorColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<Set<String>> analyzeColumnNames;
    private final Optional<Long> maxScannedFileSize;

    @JsonCreator
    public TestingConnectorTableHandle(
            String schemaName,
            String tableName,
            @JsonProperty("partitionColumns") List<TestingConnectorColumnHandle> partitionColumns,
            @JsonProperty("dataColumns") List<TestingConnectorColumnHandle> dataColumns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<TestingConnectorColumnHandle> compactEffectivePredicate,
            @JsonProperty("enforcedConstraint") TupleDomain<ColumnHandle> enforcedConstraint,
            @JsonProperty("analyzeColumnNames") Optional<Set<String>> analyzeColumnNames)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.partitionColumns = partitionColumns;
        this.tableParameters = Optional.empty();
        this.dataColumns = dataColumns;
        this.partitions = Optional.empty();
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.enforcedConstraint = enforcedConstraint;
        this.analyzeColumnNames = analyzeColumnNames;
        this.maxScannedFileSize = Optional.empty();
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public Optional<Map<String, String>> getTableParameters()
    {
        return tableParameters;
    }

    public List<TestingConnectorColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    public List<TestingConnectorColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }

    public TupleDomain<TestingConnectorColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    public Optional<Set<String>> getAnalyzeColumnNames()
    {
        return analyzeColumnNames;
    }

    public Optional<Long> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
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
        TestingConnectorTableHandle that = (TestingConnectorTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) && Objects.equals(tableName, that.tableName) && Objects.equals(tableParameters, that.tableParameters) && Objects.equals(partitionColumns, that.partitionColumns) && Objects.equals(dataColumns, that.dataColumns) && Objects.equals(partitions, that.partitions) && Objects.equals(compactEffectivePredicate, that.compactEffectivePredicate) && Objects.equals(enforcedConstraint, that.enforcedConstraint) && Objects.equals(analyzeColumnNames, that.analyzeColumnNames) && Objects.equals(maxScannedFileSize, that.maxScannedFileSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableParameters, partitionColumns, dataColumns, partitions, compactEffectivePredicate, enforcedConstraint, analyzeColumnNames, maxScannedFileSize);
    }

    @Override
    public String toString()
    {
        return "TestingConnectorTableHandle{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableParameters=" + tableParameters +
                ", partitionColumns=" + partitionColumns +
                ", dataColumns=" + dataColumns +
                ", partitions=" + partitions +
                ", compactEffectivePredicate=" + compactEffectivePredicate +
                ", enforcedConstraint=" + enforcedConstraint +
                ", analyzeColumnNames=" + analyzeColumnNames +
                ", maxScannedFileSize=" + maxScannedFileSize +
                '}';
    }
}
