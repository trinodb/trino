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

package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class InfluxTableHandle
        extends SchemaTableName
        implements ConnectorTableHandle
{
    private final String retentionPolicy;
    private final String measurement;
    private final InfluxQL where;
    private final Long limit;

    @JsonCreator
    public InfluxTableHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
            @JsonProperty("measurement") String measurement,
            @JsonProperty("where") InfluxQL where,
            @JsonProperty("limit") Long limit)
    {
        super(retentionPolicy, measurement);
        this.retentionPolicy = requireNonNull(retentionPolicy, "retentionPolicy is null");
        this.measurement = requireNonNull(measurement, "measurement is null");
        this.where = requireNonNull(where, "where is null");
        this.limit = limit;
    }

    public InfluxTableHandle(String retentionPolicy, String measurement)
    {
        this(retentionPolicy, measurement, new InfluxQL(), null);
    }

    @JsonProperty
    public String getRetentionPolicy()
    {
        return retentionPolicy;
    }

    @JsonProperty
    public String getMeasurement()
    {
        return measurement;
    }

    @JsonProperty
    public InfluxQL getWhere()
    {
        return where;
    }

    @JsonProperty
    public Long getLimit()
    {
        return limit;
    }

    public InfluxQL getFromWhere()
    {
        InfluxQL from = new InfluxQL("FROM ")
                .addIdentifier(getRetentionPolicy()).append('.').addIdentifier(getMeasurement());
        if (!getWhere().isEmpty()) {
            from.append(' ').append(getWhere().toString());
        }
        if (getLimit() != null) {
            from.append(" LIMIT ").append(getLimit());
        }
        return from;
    }

    @Override
    public String toString()
    {
        return getFromWhere().toString();
    }
}
