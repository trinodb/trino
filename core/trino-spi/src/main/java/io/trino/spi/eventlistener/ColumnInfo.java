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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.util.Objects;
import java.util.Optional;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class ColumnInfo
{
    private final String column;
    private final Optional<String> mask;

    @JsonCreator
    @Unstable
    public ColumnInfo(String column, Optional<String> mask)
    {
        this.column = column;
        this.mask = mask;
    }

    @JsonProperty
    public String getColumn()
    {
        return column;
    }

    @JsonProperty
    public Optional<String> getMask()
    {
        return mask;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ColumnInfo that)) {
            return false;
        }
        return Objects.equals(column, that.column) && Objects.equals(mask, that.mask);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, mask);
    }
}
