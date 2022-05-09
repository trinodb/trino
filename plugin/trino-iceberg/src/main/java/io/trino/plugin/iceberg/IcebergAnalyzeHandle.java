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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergAnalyzeHandle
        extends IcebergTableHandle
{
    private final Set<String> analyzeColumnNames;

    @JsonCreator
    public IcebergAnalyzeHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("tableSchemaJson") String tableSchemaJson,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("analyzeColumnNames") Set<String> analyzeColumnNames)
    {
        super(
                schemaName,
                tableName,
                TableType.DATA,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                nameMappingJson,
                tableLocation,
                storageProperties,
                RetryMode.NO_RETRIES,
                ImmutableList.of());

        this.analyzeColumnNames = ImmutableSet.copyOf(requireNonNull(analyzeColumnNames, "analyzeColumnNames is null"));
    }

    @JsonProperty
    public Set<String> getAnalyzeColumnNames()
    {
        return analyzeColumnNames;
    }
}
