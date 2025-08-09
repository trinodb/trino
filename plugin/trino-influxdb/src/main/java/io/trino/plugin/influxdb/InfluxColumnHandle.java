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

package io.trino.plugin.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind;
import static java.util.Objects.requireNonNull;

public record InfluxColumnHandle(
        @JsonProperty("columnName") String name,
        @JsonProperty("columnType") Type type,
        @JsonProperty("columnKind") ColumnKind kind)
        implements ColumnHandle
{
    @JsonCreator
    public InfluxColumnHandle(String name, Type type, ColumnKind kind)
    {
        this.name = requireNonNull(name, "columnName is null");
        this.type = requireNonNull(type, "columnType is null");
        this.kind = requireNonNull(kind, "columnKind is null");
    }
}
