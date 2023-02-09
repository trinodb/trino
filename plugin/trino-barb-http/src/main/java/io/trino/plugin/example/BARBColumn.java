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
package io.trino.plugin.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class ExampleColumn
{
    private final int station_code;
    private final String station_name;

    @JsonCreator
    public ExampleColumn(
            @JsonProperty("station_code") String station_code,
            @JsonProperty("station_name") String station_name)
    {
        //checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.station_code = station_code;
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public int getStationCode()
    {
        return station_code;
    }

    @JsonProperty
    public String getStationName()
    {
        return station_name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(station_code, station_name);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExampleColumn other = (ExampleColumn) obj;
        return Objects.equals(this.station_code, other.station_code) &&
                Objects.equals(this.station_name, other.station_name);
    }

    @Override
    public String toString()
    {
        return station_code + ":" + station_name;
    }
}
