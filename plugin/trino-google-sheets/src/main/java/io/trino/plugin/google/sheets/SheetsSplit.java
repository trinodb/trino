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
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class SheetsSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SheetsSplit.class).instanceSize();

    private final String schemaName;
    private final String tableName;
    private final List<List<String>> values;
    private final List<HostAddress> hostAddresses;

    @JsonCreator
    public SheetsSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("values") List<List<String>> values)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.values = requireNonNull(values, "values is null");
        this.hostAddresses = ImmutableList.of();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
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
        return ImmutableMap.builder()
                .put("schemaName", schemaName)
                .put("tableName", tableName)
                .put("hostAddresses", hostAddresses)
                .build();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(schemaName)
                + estimatedSizeOf(tableName)
                + estimatedSizeOf(values, value -> estimatedSizeOf(value, SizeOf::estimatedSizeOf))
                + estimatedSizeOf(hostAddresses, HostAddress::getRetainedSizeInBytes);
    }
}
