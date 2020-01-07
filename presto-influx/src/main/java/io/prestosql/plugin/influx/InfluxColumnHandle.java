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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;

public class InfluxColumnHandle
        extends InfluxColumn
        implements ColumnHandle
{
    private final String retentionPolicy;
    private final String measurement;

    @JsonCreator
    public InfluxColumnHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
            @JsonProperty("measurement") String measurement,
            @JsonProperty("influxName") String influxName,
            @JsonProperty("influxType") String influxType,
            @JsonProperty("type") Type type,
            @JsonProperty("kind") Kind kind,
            @JsonProperty("hidden") boolean hidden)
    {
        super(influxName, influxType, type, kind, hidden);
        this.retentionPolicy = retentionPolicy;
        this.measurement = measurement;
    }

    public InfluxColumnHandle(String retentionPolicy,
            String measurement,
            InfluxColumn column)
    {
        this(retentionPolicy, measurement, column.getInfluxName(), column.getInfluxType(), column.getType(), column.getKind(), column.isHidden());
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getRetentionPolicy())
                .addValue(getMeasurement())
                .toString();
    }
}
