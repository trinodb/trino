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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind;
import static java.util.Objects.requireNonNull;

public class InfluxColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final ColumnKind kind;

    @JsonCreator
    public InfluxColumnHandle(
            @JsonProperty("columnName") String name,
            @JsonProperty("columnType") Type type,
            @JsonProperty("columnKind") ColumnKind kind)
    {
        this.name = requireNonNull(name, "columnName is null");
        this.type = requireNonNull(type, "columnType is null");
        this.kind = requireNonNull(kind, "columnKind is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public ColumnKind getKind()
    {
        return kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, kind);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        InfluxColumnHandle other = (InfluxColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.kind, other.kind);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("kind", kind)
                .toString();
    }
}
