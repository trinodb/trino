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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LastCheckpoint
{
    private final long version;
    private final BigInteger size;
    private final Optional<Integer> parts;

    @JsonCreator
    public LastCheckpoint(
            @JsonProperty("version") long version,
            @JsonProperty("size") BigInteger size,
            @JsonProperty("parts") Optional<Integer> parts)
    {
        this.version = version;
        this.size = requireNonNull(size, "size is null");
        this.parts = requireNonNull(parts, "parts is null");
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public BigInteger getSize()
    {
        return size;
    }

    @JsonProperty
    public Optional<Integer> getParts()
    {
        return parts;
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
        LastCheckpoint that = (LastCheckpoint) o;
        return version == that.version &&
                size.equals(that.size) &&
                parts.equals(that.parts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, size, parts);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(version)
                .add("size", size)
                .add("parts", parts)
                .toString();
    }
}
