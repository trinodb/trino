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
package io.prestosql.execution.buffer;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.execution.buffer.PageCodecMarker.MarkerSet;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spiller.SpillCipher;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.prestosql.execution.buffer.PageCodecMarker.ENCRYPTED;
import static io.prestosql.execution.buffer.PagesSerdeUtil.readRawPage;
import static io.prestosql.execution.buffer.PagesSerdeUtil.writeRawPage;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PagesSerde
{
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

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

    public SerializedPage serialize(Page page)
    {
        SliceOutput serializationBuffer = new DynamicSliceOutput(toIntExact(page.getSizeInBytes() + Integer.BYTES)); // block length is an int
        writeRawPage(page, serializationBuffer, blockEncodingSerde);
        Slice slice = serializationBuffer.slice();
        int uncompressedSize = serializationBuffer.size();
        MarkerSet markers = MarkerSet.empty();

        if (compressor.isPresent()) {
            byte[] compressed = new byte[compressor.get().maxCompressedLength(uncompressedSize)];
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
            }
        }

        if (spillCipher.isPresent()) {
            byte[] encrypted = new byte[spillCipher.get().encryptedMaxLength(slice.length())];
            int encryptedSize = spillCipher.get().encrypt(
                    slice.byteArray(),
                    slice.byteArrayOffset(),
                    slice.length(),
                    encrypted,
                    0);

            slice = Slices.wrappedBuffer(encrypted, 0, encryptedSize);
            markers.add(ENCRYPTED);
        }

        if (!slice.isCompact()) {
            slice = Slices.copyOf(slice);
        }

        return new SerializedPage(slice, markers, page.getPositionCount(), uncompressedSize);
    }

    public Page deserialize(SerializedPage serializedPage)
    {
        checkArgument(serializedPage != null, "serializedPage is null");

        Slice slice = serializedPage.getSlice();

        if (serializedPage.isEncrypted()) {
            checkState(spillCipher.isPresent(), "Page is encrypted, but spill cipher is missing");

            byte[] decrypted = new byte[spillCipher.get().decryptedMaxLength(slice.length())];
            int decryptedSize = spillCipher.get().decrypt(
                    slice.byteArray(),
                    slice.byteArrayOffset(),
                    slice.length(),
                    decrypted,
                    0);

            slice = Slices.wrappedBuffer(decrypted, 0, decryptedSize);
        }

        if (serializedPage.isCompressed()) {
            checkState(decompressor.isPresent(), "Page is compressed, but decompressor is missing");

            int uncompressedSize = serializedPage.getUncompressedSizeInBytes();
            byte[] decompressed = new byte[uncompressedSize];
            checkState(decompressor.get().decompress(
                    slice.byteArray(),
                    slice.byteArrayOffset(),
                    slice.length(),
                    decompressed,
                    0,
                    uncompressedSize) == uncompressedSize);

            slice = Slices.wrappedBuffer(decompressed);
        }

        return readRawPage(serializedPage.getPositionCount(), slice.getInput(), blockEncodingSerde);
    }
}
