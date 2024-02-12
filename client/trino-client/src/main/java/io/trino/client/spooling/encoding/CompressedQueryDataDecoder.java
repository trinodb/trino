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
package io.trino.client.spooling.encoding;

import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttribute;
import io.trino.client.spooling.DataAttributes;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class CompressedQueryDataDecoder
        implements QueryDataDecoder
{
    protected final QueryDataDecoder delegate;

    public CompressedQueryDataDecoder(QueryDataDecoder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    abstract InputStream decompress(InputStream inputStream, int uncompressedSize)
            throws IOException;

    @Override
    public Iterable<List<Object>> decode(InputStream stream, DataAttributes metadata)
            throws IOException
    {
        Optional<Integer> uncompressedSize = metadata.getOptional(DataAttribute.UNCOMPRESSED_SIZE, Integer.class);
        if (uncompressedSize.isPresent()) {
            return delegate.decode(decompress(stream, uncompressedSize.get()), metadata);
        }
        // Data not compressed - below threshold
        return delegate.decode(stream, metadata);
    }
}
