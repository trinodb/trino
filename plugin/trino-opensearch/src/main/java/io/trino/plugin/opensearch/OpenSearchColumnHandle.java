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
package io.trino.plugin.opensearch;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record OpenSearchColumnHandle(
        List<String> path,
        Type type,
        DecoderDescriptor decoderDescriptor,
        boolean supportsPredicates,
        String dereferencePath)
        implements ColumnHandle
{
    public OpenSearchColumnHandle
    {
        path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        requireNonNull(type, "type is null");
        requireNonNull(decoderDescriptor, "decoderDescriptor is null");
        checkArgument(Joiner.on('.').join(ImmutableList.of(path)).equals(dereferencePath), "dereferencePath doesn't match path");
    }

    public OpenSearchColumnHandle(
            List<String> path,
            Type type,
            DecoderDescriptor decoderDescriptor,
            boolean supportsPredicates)
    {
        this(path, type, decoderDescriptor, supportsPredicates, Joiner.on('.').join(ImmutableList.of(path)));
    }

    @Override
    public String toString()
    {
        return dereferencePath + "::" + type();
    }
}
