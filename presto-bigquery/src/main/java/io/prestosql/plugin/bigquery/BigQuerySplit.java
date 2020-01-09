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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;

public class BigQuerySplit
        implements ConnectorSplit
{
    private final String streamName;
    private final String avroSchema;
    private final List<ColumnHandle> columns;

    @JsonCreator
    public BigQuerySplit(
            @JsonProperty("streamName") String streamName,
            @JsonProperty("avroSchema") String avroSchema,
            @JsonProperty("columns") List<ColumnHandle> columns)
    {
        this.streamName = streamName;
        this.avroSchema = avroSchema;
        this.columns = columns;
    }

    @JsonProperty
    public String getStreamName()
    {
        return streamName;
    }

    @JsonProperty
    public String getAvroSchema()
    {
        return avroSchema;
    }

    @JsonProperty
    public List<ColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
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
        BigQuerySplit that = (BigQuerySplit) o;
        return streamName.equals(that.streamName) &&
                avroSchema.equals(that.avroSchema) &&
                columns.equals(that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(streamName, avroSchema, columns);
    }

    @Override
    public String toString()
    {
        return "BigQuerySplit{" +
                "streamName='" + streamName + '\'' +
                ", avroSchema='" + avroSchema + '\'' +
                ", columns=" + columns +
                '}';
    }
}
