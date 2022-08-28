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
package io.trino.hive.formats.compression;

import java.util.function.Supplier;

public class NoneCompressor
        implements Compressor
{
    @Override
    public MemoryCompressedSliceOutput createCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new NoneCompressedSliceOutputSupplier(minChunkSize, maxChunkSize).get();
    }

    private static class NoneCompressedSliceOutputSupplier
            implements Supplier<MemoryCompressedSliceOutput>
    {
        private final ChunkedSliceOutput chunkedSliceOutput;

        private NoneCompressedSliceOutputSupplier(int minChunkSize, int maxChunkSize)
        {
            chunkedSliceOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public MemoryCompressedSliceOutput get()
        {
            chunkedSliceOutput.reset();
            return new MemoryCompressedSliceOutput(chunkedSliceOutput, chunkedSliceOutput, this, () -> {});
        }
    }
}
