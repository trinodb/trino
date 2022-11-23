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
package io.trino.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.bigquery.BigQueryTableHandle.BigQueryPartitionType;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BigQueryNamedRelationHandle
        extends BigQueryRelationHandle
{
    private final SchemaTableName schemaTableName;
    private final RemoteTableName remoteTableName;
    private final String type;
    private final Optional<BigQueryPartitionType> partitionType;
    private final Optional<String> comment;

    @JsonCreator
    public BigQueryNamedRelationHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("type") String type,
            @JsonProperty("partitionType") Optional<BigQueryPartitionType> partitionType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.type = requireNonNull(type, "type is null");
        this.partitionType = requireNonNull(partitionType, "partitionType is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<BigQueryPartitionType> getPartitionType()
    {
        return partitionType;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
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
        BigQueryNamedRelationHandle that = (BigQueryNamedRelationHandle) o;
        // NOTE remoteTableName is not compared here because two handles differing in only remoteTableName will create ambiguity
        // TODO: Add tests for this (see TestJdbcTableHandle#testEquivalence for reference)
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(type, that.type) &&
                Objects.equals(partitionType, that.partitionType) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, type, partitionType, comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("remoteTableName", remoteTableName)
                .add("schemaTableName", schemaTableName)
                .add("type", type)
                .add("comment", comment)
                .toString();
    }
}
