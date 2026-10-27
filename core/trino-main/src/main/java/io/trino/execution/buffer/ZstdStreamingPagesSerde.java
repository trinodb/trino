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

import com.google.common.collect.ImmutableMap;
import io.airlift.compress.v3.zstd.ZstdInputStream;
import io.airlift.compress.v3.zstd.ZstdOutputStream;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.metrics.Metrics;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_CIPHER_NAME;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PagesSerdeUtil.readRawPage;
import static io.trino.execution.buffer.PagesSerdeUtil.writeRawPage;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * Experimental exchange format: the standard page header followed by one Zstd frame.
 * With encryption, the payload is an IV followed by the encrypted frame. Each page is
 * independent, and the codec is selected out of band, just like the block codecs.
 */
final class ZstdStreamingPagesSerde
        implements PageSerializer, PageDeserializer
{
    private static final int INSTANCE_SIZE = instanceSize(ZstdStreamingPagesSerde.class);
    private static final int BUFFER_SIZE = 64 * 1024;

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<SecretKey> encryptionKey;
    private long inputBytes;
    private long outputBytes;

    ZstdStreamingPagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<SecretKey> encryptionKey)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        encryptionKey.ifPresent(key -> checkArgument(is256BitSecretKeySpec(key), "encryptionKey must contain a 256bit AES key"));
    }

    @Override
    public Slice serialize(Page page)
    {
        // Grow with compressed data instead of reserving space for the uncompressed page.
        DynamicSliceOutput output = new DynamicSliceOutput(BUFFER_SIZE);
        output.writeInt(page.getPositionCount());
        output.writeInt(0);
        output.writeInt(0);
        try {
            OutputStream payload = output;
            if (encryptionKey.isPresent()) {
                Cipher cipher = Cipher.getInstance(SERIALIZED_PAGE_CIPHER_NAME);
                cipher.init(ENCRYPT_MODE, encryptionKey.orElseThrow());
                output.writeBytes(cipher.getIV());
                payload = new CipherOutputStream(output, cipher);
            }
            int uncompressedSize;
            try (OutputStreamSliceOutput rawOutput = new OutputStreamSliceOutput(ZstdOutputStream.create(payload), BUFFER_SIZE)) {
                writeRawPage(page, rawOutput, blockEncodingSerde);
                uncompressedSize = rawOutput.size();
            }
            Slice serialized = output.slice();
            int compressedSize = serialized.length() - SERIALIZED_PAGE_HEADER_SIZE;
            serialized.setInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET, uncompressedSize);
            serialized.setInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET, compressedSize);
            inputBytes += uncompressedSize;
            outputBytes += compressedSize;
            return serialized;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encrypt streaming page", e);
        }
    }

    @Override
    public Page deserialize(Slice serialized)
    {
        checkArgument(serialized.length() >= SERIALIZED_PAGE_HEADER_SIZE, "Truncated page header");
        int positionCount = getSerializedPagePositionCount(serialized);
        int uncompressedSize = serialized.getInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET);
        int compressedSize = serialized.getInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET);
        checkArgument(positionCount >= 0, "Invalid position count");
        checkArgument(uncompressedSize >= Integer.BYTES, "Invalid uncompressed page size");
        checkArgument(compressedSize == serialized.length() - SERIALIZED_PAGE_HEADER_SIZE, "Invalid compressed page size");
        try {
            InputStream payload = serialized.slice(SERIALIZED_PAGE_HEADER_SIZE, compressedSize).getInput();
            if (encryptionKey.isPresent()) {
                Cipher cipher = Cipher.getInstance(SERIALIZED_PAGE_CIPHER_NAME);
                byte[] iv = payload.readNBytes(cipher.getBlockSize());
                checkArgument(iv.length == cipher.getBlockSize(), "Truncated page IV");
                cipher.init(DECRYPT_MODE, encryptionKey.orElseThrow(), new IvParameterSpec(iv));
                payload = new CipherInputStream(payload, cipher);
            }
            try (InputStreamSliceInput input = new InputStreamSliceInput(ZstdInputStream.create(payload), BUFFER_SIZE)) {
                Page page = readRawPage(positionCount, input, blockEncodingSerde);
                checkArgument(input.position() == uncompressedSize, "Invalid uncompressed page size");
                // Consume the end of the frame, including its checksum and any encryption padding.
                checkArgument(input.read() == -1, "Trailing page data");
                return page;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decrypt streaming page", e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // Streams and their buffers are closed and released within each operation.
        return INSTANCE_SIZE + encryptionKey.map(_ -> instanceSize(SecretKeySpec.class) + sizeOfByteArray(32)).orElse(0L);
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(
                "exchangeSerializerInputBytes", new LongCount(inputBytes),
                "exchangeSerializerOutputBytes", new LongCount(outputBytes)));
    }

    @Override
    public Metrics getAndResetMetrics()
    {
        Metrics metrics = getMetrics();
        inputBytes = 0;
        outputBytes = 0;
        return metrics;
    }
}
