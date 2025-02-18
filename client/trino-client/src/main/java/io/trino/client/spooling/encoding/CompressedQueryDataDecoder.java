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

import com.google.common.io.ByteStreams;
import io.trino.client.CloseableIterator;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttribute;
import io.trino.client.spooling.DataAttributes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public abstract class CompressedQueryDataDecoder
        implements QueryDataDecoder
{
    protected final QueryDataDecoder delegate;

    public CompressedQueryDataDecoder(QueryDataDecoder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    abstract void decompress(byte[] input, byte[] output)
            throws IOException;

    @Override
    public CloseableIterator<List<Object>> decode(InputStream stream, DataAttributes metadata)
            throws IOException
    {
        Optional<Integer> expectedDecompressedSize = metadata.getOptional(DataAttribute.UNCOMPRESSED_SIZE, Integer.class);
        int segmentSize = metadata.get(DataAttribute.SEGMENT_SIZE, Integer.class);

        if (expectedDecompressedSize.isPresent()) {
            int uncompressedSize = expectedDecompressedSize.get();
            try (InputStream inputStream = stream) {
                byte[] input = new byte[segmentSize];
                byte[] output = new byte[uncompressedSize];
                int readBytes = ByteStreams.read(inputStream, input, 0, segmentSize);
                verify(readBytes == segmentSize, "Expected to read %s bytes but got %s", segmentSize, readBytes);
                decompress(input, output);
                return delegate.decode(new ByteArrayInputStream(output), metadata);
            }
        }
        // Data not compressed - below threshold
        return delegate.decode(stream, metadata);
    }
}
