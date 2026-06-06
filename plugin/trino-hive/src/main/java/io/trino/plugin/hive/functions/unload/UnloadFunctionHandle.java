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
package io.trino.plugin.hive.functions.unload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record UnloadFunctionHandle(
        String location,
        HiveStorageFormat storageFormat,
        List<String> columnNames,
        List<Type> columnTypes)
        implements ConnectorTableFunctionHandle
{
    @JsonCreator
    public UnloadFunctionHandle(
            @JsonProperty("location") String location,
            @JsonProperty("storageFormat") HiveStorageFormat storageFormat,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes)
    {
        this.location = requireNonNull(location, "location is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.columnNames = List.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
    }
}
