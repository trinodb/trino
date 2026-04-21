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
package io.trino.plugin.memory;

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.HostAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public record MemoryDataFragment(HostAddress hostAddress, long rows)
{
    private static final JsonCodec<MemoryDataFragment> MEMORY_DATA_FRAGMENT_CODEC = jsonCodec(MemoryDataFragment.class);

    public MemoryDataFragment
    {
        requireNonNull(hostAddress, "hostAddress is null");
        checkArgument(rows >= 0, "Rows number cannot be negative");
    }

    public Slice toSlice()
    {
        return Slices.wrappedBuffer(MEMORY_DATA_FRAGMENT_CODEC.toJsonBytes(this));
    }

    public static MemoryDataFragment fromSlice(Slice fragment)
    {
        return MEMORY_DATA_FRAGMENT_CODEC.fromJson(fragment.getInput());
    }

    public static MemoryDataFragment merge(MemoryDataFragment a, MemoryDataFragment b)
    {
        checkArgument(a.hostAddress().equals(b.hostAddress()), "Cannot merge fragments from different hosts");
        return new MemoryDataFragment(a.hostAddress(), a.rows() + b.rows());
    }
}
