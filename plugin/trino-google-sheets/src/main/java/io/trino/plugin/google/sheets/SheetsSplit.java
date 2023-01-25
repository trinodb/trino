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
package io.trino.plugin.google.sheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SheetsSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SheetsSplit.class);

    private final Optional<String> schemaName;
    private final Optional<String> tableName;
    private final Optional<String> sheetExpression;
    private final List<List<String>> values;
    private final List<HostAddress> hostAddresses;

    @JsonCreator
    public SheetsSplit(
            @JsonProperty("schemaName") Optional<String> schemaName,
            @JsonProperty("tableName") Optional<String> tableName,
            @JsonProperty("sheetExpression") Optional<String> sheetExpression,
            @JsonProperty("values") List<List<String>> values)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.sheetExpression = requireNonNull(sheetExpression, "sheetExpression is null");
        this.values = requireNonNull(values, "values is null");
        this.hostAddresses = ImmutableList.of();
    }

    @JsonProperty
    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public Optional<String> getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<String> getSheetExpression()
    {
        return sheetExpression;
    }

    @JsonProperty
    public List<List<String>> getValues()
    {
        return values;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return hostAddresses;
    }

    @Override
    public Object getInfo()
    {
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                .put("hostAddresses", hostAddresses);
        schemaName.ifPresent(name -> builder.put("schemaName", name));
        tableName.ifPresent(name -> builder.put("tableName", name));
        sheetExpression.ifPresent(expression -> builder.put("sheetExpression", expression));
        return builder.buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(schemaName, SizeOf::estimatedSizeOf)
                + sizeOf(tableName, SizeOf::estimatedSizeOf)
                + sizeOf(sheetExpression, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(values, value -> estimatedSizeOf(value, SizeOf::estimatedSizeOf))
                + estimatedSizeOf(hostAddresses, HostAddress::getRetainedSizeInBytes);
    }
}
