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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.pulsar.util.OffloadPoliciesImpl;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PulsarSplit
        implements ConnectorSplit
{
    private final long splitId;
    private final String catalogName;
    private final String tableName;
    private final long splitSize;
    private final TupleDomain<ColumnHandle> tupleDomain;

    private final String schemaName;
    private final String originSchemaName;
    private final String schema;
    private final SchemaType schemaType;
    private final SchemaInfo schemaInfo;

    private final long startPositionLedgerId;
    private final long startPositionEntryId;
    private final long endPositionLedgerId;
    private final long endPositionEntryId;
    private final PositionImpl startPosition;
    private final PositionImpl endPosition;
    private final String schemaInfoProperties;

    private final OffloadPoliciesImpl offloadPolicies;

    @JsonCreator
    public PulsarSplit(
            @JsonProperty("splitId") long splitId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("originSchemaName") String originSchemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("splitSize") long splitSize,
            @JsonProperty("schema") String schema,
            @JsonProperty("schemaType") SchemaType schemaType,
            @JsonProperty("startPositionEntryId") long startPositionEntryId,
            @JsonProperty("endPositionEntryId") long endPositionEntryId,
            @JsonProperty("startPositionLedgerId") long startPositionLedgerId,
            @JsonProperty("endPositionLedgerId") long endPositionLedgerId,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("schemaInfoProperties") String schemaInfoProperties,
            @JsonProperty("offloadPolicies") OffloadPoliciesImpl offloadPolicies)
            throws IOException
    {
        this.splitId = splitId;
        requireNonNull(schemaName, "schema name is null");
        this.originSchemaName = originSchemaName;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.splitSize = splitSize;
        this.schema = schema;
        this.schemaType = schemaType;
        this.startPositionEntryId = startPositionEntryId;
        this.endPositionEntryId = endPositionEntryId;
        this.startPositionLedgerId = startPositionLedgerId;
        this.endPositionLedgerId = endPositionLedgerId;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.startPosition = PositionImpl.get(startPositionLedgerId, startPositionEntryId);
        this.endPosition = PositionImpl.get(endPositionLedgerId, endPositionEntryId);
        this.schemaInfoProperties = schemaInfoProperties;
        this.offloadPolicies = offloadPolicies;

        ObjectMapper objectMapper = new ObjectMapper();
        this.schemaInfo = SchemaInfoImpl.builder()
                .name(originSchemaName)
                .type(schemaType)
                .schema(schema.getBytes(StandardCharsets.ISO_8859_1))
                .properties(objectMapper.readValue(schemaInfoProperties, Map.class))
                .build();
    }

    @JsonProperty
    public long getSplitId()
    {
        return splitId;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public SchemaType getSchemaType()
    {
        return schemaType;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getSplitSize()
    {
        return splitSize;
    }

    @JsonProperty
    public String getOriginSchemaName()
    {
        return originSchemaName;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public long getStartPositionEntryId()
    {
        return startPositionEntryId;
    }

    @JsonProperty
    public long getEndPositionEntryId()
    {
        return endPositionEntryId;
    }

    @JsonProperty
    public long getStartPositionLedgerId()
    {
        return startPositionLedgerId;
    }

    @JsonProperty
    public long getEndPositionLedgerId()
    {
        return endPositionLedgerId;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    public PositionImpl getStartPosition()
    {
        return startPosition;
    }

    public PositionImpl getEndPosition()
    {
        return endPosition;
    }

    @JsonProperty
    public String getSchemaInfoProperties()
    {
        return schemaInfoProperties;
    }

    @JsonProperty
    public OffloadPoliciesImpl getOffloadPolicies()
    {
        return offloadPolicies;
    }

    public org.apache.pulsar.shade.org.apache.pulsar.common.policies.data.OffloadPoliciesImpl getOffloadPoliciesOriginType()
    {
        return offloadPolicies != null ? org.apache.pulsar.shade.org.apache.pulsar.common.policies.data
                .OffloadPoliciesImpl.create(offloadPolicies.toProperties()) : null;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(HostAddress.fromParts("localhost", 12345));
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "PulsarSplit{"
                + "splitId=" + splitId
                + ", catalogName='" + catalogName + '\''
                + ", originSchemaName='" + originSchemaName + '\''
                + ", schemaName='" + schemaName + '\''
                + ", tableName='" + tableName + '\''
                + ", splitSize=" + splitSize
                + ", schema='" + schema + '\''
                + ", schemaType=" + schemaType
                + ", startPositionEntryId=" + startPositionEntryId
                + ", endPositionEntryId=" + endPositionEntryId
                + ", startPositionLedgerId=" + startPositionLedgerId
                + ", endPositionLedgerId=" + endPositionLedgerId
                + ", schemaInfoProperties=" + schemaInfoProperties
                + (offloadPolicies == null ? "" : offloadPolicies.toString())
                + '}';
    }

    public SchemaInfo getSchemaInfo()
    {
        return schemaInfo;
    }
}
