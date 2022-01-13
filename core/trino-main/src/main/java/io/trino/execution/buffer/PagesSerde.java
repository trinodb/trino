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
package io.trino.execution.buffer;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.execution.buffer.PageCodecMarker.MarkerSet;
import io.trino.spi.Page;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spiller.SpillCipher;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.trino.execution.buffer.PageCodecMarker.ENCRYPTED;
import static io.trino.execution.buffer.PagesSerdeUtil.readRawPage;
import static io.trino.execution.buffer.PagesSerdeUtil.writeRawPage;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PagesSerde
{
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;
    private static final int SERIALIZED_PAGE_HEADER_SIZE = /*positionCount*/ Integer.BYTES +
            // pageCodecMarkers
            Byte.BYTES +
            // uncompressedSizeInBytes
            Integer.BYTES +
            // sizeInBytes
            Integer.BYTES;

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<Compressor> compressor;
    private final Optional<Decompressor> decompressor;
    private final Optional<SpillCipher> spillCipher;

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<Compressor> compressor, Optional<Decompressor> decompressor, Optional<SpillCipher> spillCipher)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        checkArgument(compressor.isPresent() == decompressor.isPresent(), "compressor and decompressor must both be present or both be absent");
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.spillCipher = requireNonNull(spillCipher, "spillCipher is null");
    }

    public PagesSerdeContext newContext()
    {
        return new PagesSerdeContext();
    }

    public Slice serialize(PagesSerdeContext context, Page page)
    {
        DynamicSliceOutput serializationBuffer = context.acquireSliceOutput(toIntExact(page.getSizeInBytes() + Integer.BYTES)); // block length is an int
        byte[] inUseTempBuffer = null;
        try {
            writeRawPage(page, serializationBuffer, blockEncodingSerde);
            Slice slice = serializationBuffer.slice();
            int uncompressedSize = serializationBuffer.size();
            MarkerSet markers = MarkerSet.empty();

            if (compressor.isPresent()) {
                byte[] compressed = context.acquireBuffer(compressor.get().maxCompressedLength(uncompressedSize));
                int compressedSize = compressor.get().compress(
                        slice.byteArray(),
                        slice.byteArrayOffset(),
                        uncompressedSize,
                        compressed,
                        0,
                        compressed.length);

                if ((((double) compressedSize) / uncompressedSize) <= MINIMUM_COMPRESSION_RATIO) {
                    slice = Slices.wrappedBuffer(compressed, 0, compressedSize);
                    markers.add(COMPRESSED);
                    inUseTempBuffer = compressed; // Track the compression buffer as in use
                }
                else {
                    // Eager release of the compression buffer to enable reusing it for encryption without an extra allocation
                    context.releaseBuffer(compressed);
                }
            }

            if (spillCipher.isPresent()) {
                byte[] encrypted = context.acquireBuffer(spillCipher.get().encryptedMaxLength(slice.length()));
                int encryptedSize = spillCipher.get().encrypt(
                        slice.byteArray(),
                        slice.byteArrayOffset(),
                        slice.length(),
                        encrypted,
                        0);

                slice = Slices.wrappedBuffer(encrypted, 0, encryptedSize);
                markers.add(ENCRYPTED);
                //  Previous buffer is no longer in use and can be released
                if (inUseTempBuffer != null) {
                    context.releaseBuffer(inUseTempBuffer);
                }
                inUseTempBuffer = encrypted;
            }

            SliceOutput output = Slices.allocate(SERIALIZED_PAGE_HEADER_SIZE + slice.length()).getOutput();
            output.writeInt(page.getPositionCount());
            output.writeByte(markers.byteValue());
            output.writeInt(uncompressedSize);
            output.writeInt(slice.length());
            output.writeBytes(slice);

            return output.getUnderlyingSlice();
        }
        finally {
            context.releaseSliceOutput(serializationBuffer);
            if (inUseTempBuffer != null) {
                context.releaseBuffer(inUseTempBuffer);
            }
        }
    }

    public static int getSerializedPagePositionCount(Slice serializedPage)
    {
        return serializedPage.getInt(0);
    }

    public static boolean isSerializedPageEncrypted(Slice serializedPage)
    {
        return getSerializedPageMarkerSet(serializedPage).contains(ENCRYPTED);
    }

    public static boolean isSerializedPageCompressed(Slice serializedPage)
    {
        return getSerializedPageMarkerSet(serializedPage).contains(COMPRESSED);
    }

    private static MarkerSet getSerializedPageMarkerSet(Slice serializedPage)
    {
        return MarkerSet.fromByteValue(serializedPage.getByte(Integer.BYTES));
    }

    public Page deserialize(Slice serializedPage)
    {
        try (PagesSerdeContext context = newContext()) {
            return deserialize(context, serializedPage);
        }
    }

    public Page deserialize(PagesSerdeContext context, Slice serializedPage)
    {
        checkArgument(serializedPage != null, "serializedPage is null");

        SliceInput input = serializedPage.getInput();
        int positionCount = input.readInt();
        MarkerSet markers = MarkerSet.fromByteValue(input.readByte());
        int uncompressedSize = input.readInt();
        int compressedSize = input.readInt();
        Slice slice = input.readSlice(compressedSize);

        // This buffer *must not* be released at the end, since block decoding might create references to the buffer but
        // *can* be released for reuse if used for decryption and later released after decompression
        byte[] inUseTempBuffer = null;
        if (markers.contains(ENCRYPTED)) {
            checkState(spillCipher.isPresent(), "Page is encrypted, but spill cipher is missing");

            byte[] decrypted = context.acquireBuffer(spillCipher.get().decryptedMaxLength(slice.length()));
            int decryptedSize = spillCipher.get().decrypt(
                    slice.byteArray(),
                    slice.byteArrayOffset(),
                    slice.length(),
                    decrypted,
                    0);

            slice = Slices.wrappedBuffer(decrypted, 0, decryptedSize);
            inUseTempBuffer = decrypted;
        }

        if (markers.contains(COMPRESSED)) {
            checkState(decompressor.isPresent(), "Page is compressed, but decompressor is missing");

            byte[] decompressed = context.acquireBuffer(uncompressedSize);
            checkState(decompressor.get().decompress(
                    slice.byteArray(),
                    slice.byteArrayOffset(),
                    slice.length(),
                    decompressed,
                    0,
                    uncompressedSize) == uncompressedSize);

            slice = Slices.wrappedBuffer(decompressed, 0, uncompressedSize);
            if (inUseTempBuffer != null) {
                //  Previous buffer is no longer in use and safe to release
                context.releaseBuffer(inUseTempBuffer);
            }
        }

        return readRawPage(positionCount, slice.getInput(), blockEncodingSerde);
    }

    public static Slice readSerializedPage(SliceInput input)
    {
        int positionCount = input.readInt();
        byte marker = input.readByte();
        int uncompressedSize = input.readInt();
        int compressedSize = input.readInt();

        SliceOutput output = Slices.allocate(SERIALIZED_PAGE_HEADER_SIZE + compressedSize).getOutput();
        output.writeInt(positionCount);
        output.writeByte(marker);
        output.writeInt(uncompressedSize);
        output.writeInt(compressedSize);

        Slice result = output.getUnderlyingSlice();
        input.readBytes(result, SERIALIZED_PAGE_HEADER_SIZE, compressedSize);

        return result;
    }

    public static final class PagesSerdeContext
            implements AutoCloseable
    {
        //  Limit retained buffers to 4x the default max page size
        private static final int MAX_BUFFER_RETAINED_SIZE = DEFAULT_MAX_PAGE_SIZE_IN_BYTES * 4;

        private DynamicSliceOutput sliceOutput;
        //  Wraps two buffers since encryption + decryption will use at most 2 buffers at once. Buffers are kept in relative order
        //  based on length so that they can be used for compression or encryption, maximizing reuse opportunities
        private byte[] largerBuffer;
        private byte[] smallerBuffer;
        private boolean closed;

        private void checkNotClosed()
        {
            if (closed) {
                throw new IllegalStateException("PagesSerdeContext is already closed");
            }
        }

        private DynamicSliceOutput acquireSliceOutput(int estimatedSize)
        {
            checkNotClosed();
            if (sliceOutput != null && sliceOutput.writableBytes() >= estimatedSize) {
                DynamicSliceOutput result = this.sliceOutput;
                this.sliceOutput = null;
                return result;
            }
            this.sliceOutput = null; // Clear any existing slice output that might be smaller than the request
            return new DynamicSliceOutput(estimatedSize);
        }

        private void releaseSliceOutput(DynamicSliceOutput sliceOutput)
        {
            if (closed) {
                return;
            }
            sliceOutput.reset();
            if (sliceOutput.writableBytes() <= MAX_BUFFER_RETAINED_SIZE) {
                this.sliceOutput = sliceOutput;
            }
        }

        private byte[] acquireBuffer(int size)
        {
            checkNotClosed();
            byte[] result;
            //  Check the smallest buffer first
            if (smallerBuffer != null && smallerBuffer.length >= size) {
                result = smallerBuffer;
                smallerBuffer = null;
                return result;
            }
            if (largerBuffer != null && largerBuffer.length >= size) {
                result = largerBuffer;
                largerBuffer = smallerBuffer;
                smallerBuffer = null;
                return result;
            }
            return new byte[size];
        }

        private void releaseBuffer(byte[] buffer)
        {
            int size = buffer.length;
            if (closed || size > MAX_BUFFER_RETAINED_SIZE) {
                return;
            }
            if (largerBuffer == null) {
                largerBuffer = buffer;
            }
            else if (size > largerBuffer.length) {
                smallerBuffer = largerBuffer;
                largerBuffer = buffer;
            }
            else if (smallerBuffer == null || size >= smallerBuffer.length) {
                smallerBuffer = buffer;
            }
        }

        @Override
        public void close()
        {
            closed = true;
            sliceOutput = null;
            smallerBuffer = null;
            largerBuffer = null;
        }
    }
}
