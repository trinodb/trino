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

import com.google.common.base.VerifyException;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4RawCompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.trino.execution.buffer.PageCodecMarker.ENCRYPTED;
import static io.trino.execution.buffer.PagesSerdeUtil.ESTIMATED_AES_CIPHER_RETAINED_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_CIPHER_NAME;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.writeRawPage;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class PageSerializer
{
    private static final int INSTANCE_SIZE = instanceSize(PageSerializer.class);

    private final BlockEncodingSerde blockEncodingSerde;
    private final SerializedPageOutput output;

    public PageSerializer(
            BlockEncodingSerde blockEncodingSerde,
            boolean compressionEnabled,
            Optional<SecretKey> encryptionKey,
            int blockSizeInBytes)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
        encryptionKey.ifPresent(secretKey -> checkArgument(is256BitSecretKeySpec(secretKey), "encryptionKey is expected to be an instance of SecretKeySpec containing a 256bit key"));
        output = new SerializedPageOutput(
                compressionEnabled ? Optional.of(new Lz4Compressor()) : Optional.empty(),
                encryptionKey,
                blockSizeInBytes);
    }

    public Slice serialize(Page page)
    {
        output.startPage(page.getPositionCount(), toIntExact(page.getSizeInBytes()));
        writeRawPage(page, output, blockEncodingSerde);
        return output.closePage();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + output.getRetainedSize();
    }

    private static class SerializedPageOutput
            extends SliceOutput
    {
        private static final int INSTANCE_SIZE = instanceSize(SerializedPageOutput.class);
        // TODO: implement getRetainedSizeInBytes in Lz4Compressor
        private static final int COMPRESSOR_RETAINED_SIZE = toIntExact(instanceSize(Lz4Compressor.class) + sizeOfIntArray(Lz4RawCompressor.MAX_TABLE_SIZE));
        private static final int ENCRYPTION_KEY_RETAINED_SIZE = toIntExact(instanceSize(SecretKeySpec.class) + sizeOfByteArray(256 / 8));

        private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

        private final Optional<Lz4Compressor> compressor;
        private final Optional<SecretKey> encryptionKey;
        private final int markers;
        private final Optional<Cipher> cipher;

        private final WriteBuffer[] buffers;
        private int uncompressedSize;

        private SerializedPageOutput(
                Optional<Lz4Compressor> compressor,
                Optional<SecretKey> encryptionKey,
                int blockSizeInBytes)
        {
            this.compressor = requireNonNull(compressor, "compressor is null");
            this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");

            buffers = new WriteBuffer[
                    (compressor.isPresent() ? 1 : 0) // compression buffer
                            + (encryptionKey.isPresent() ? 1 : 0) // encryption buffer
                            + 1 // output buffer
                    ];
            PageCodecMarker.MarkerSet markerSet = PageCodecMarker.MarkerSet.empty();
            if (compressor.isPresent()) {
                buffers[0] = new WriteBuffer(blockSizeInBytes);
                markerSet.add(COMPRESSED);
            }
            if (encryptionKey.isPresent()) {
                int bufferSize = blockSizeInBytes;
                if (compressor.isPresent()) {
                    bufferSize = compressor.get().maxCompressedLength(blockSizeInBytes)
                            // to store compressed block size
                            + Integer.BYTES;
                }
                buffers[buffers.length - 2] = new WriteBuffer(bufferSize);
                markerSet.add(ENCRYPTED);

                try {
                    cipher = Optional.of(Cipher.getInstance(SERIALIZED_PAGE_CIPHER_NAME));
                }
                catch (GeneralSecurityException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create cipher: " + e.getMessage(), e);
                }
            }
            else {
                cipher = Optional.empty();
            }
            markers = markerSet.byteValue();
        }

        public void startPage(int positionCount, int sizeInBytes)
        {
            WriteBuffer buffer = new WriteBuffer(round(sizeInBytes * 1.2F) + SERIALIZED_PAGE_HEADER_SIZE);
            buffer.writeInt(positionCount);
            buffer.writeByte(markers);
            // leave space for uncompressed and compressed sizes
            buffer.skip(Integer.BYTES * 2);

            buffers[buffers.length - 1] = buffer;
            uncompressedSize = 0;
        }

        @Override
        public void writeByte(int value)
        {
            ensureCapacityFor(Byte.BYTES);
            buffers[0].writeByte(value);
            uncompressedSize += Byte.BYTES;
        }

        @Override
        public void writeShort(int value)
        {
            ensureCapacityFor(Short.BYTES);
            buffers[0].writeShort(value);
            uncompressedSize += Short.BYTES;
        }

        @Override
        public void writeInt(int value)
        {
            ensureCapacityFor(Integer.BYTES);
            buffers[0].writeInt(value);
            uncompressedSize += Integer.BYTES;
        }

        @Override
        public void writeLong(long value)
        {
            ensureCapacityFor(Long.BYTES);
            buffers[0].writeLong(value);
            uncompressedSize += Long.BYTES;
        }

        @Override
        public void writeFloat(float value)
        {
            ensureCapacityFor(Float.BYTES);
            buffers[0].writeFloat(value);
            uncompressedSize += Float.BYTES;
        }

        @Override
        public void writeDouble(double value)
        {
            ensureCapacityFor(Double.BYTES);
            buffers[0].writeDouble(value);
            uncompressedSize += Double.BYTES;
        }

        @Override
        public void writeBytes(Slice source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int bytesRemaining = length;
            while (bytesRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, bytesRemaining));
                int bufferCapacity = buffer.remainingCapacity();
                int bytesToCopy = min(bytesRemaining, bufferCapacity);
                buffer.writeBytes(source, currentIndex, bytesToCopy);
                currentIndex += bytesToCopy;
                bytesRemaining -= bytesToCopy;
            }
            uncompressedSize += length;
        }

        @Override
        public void writeBytes(byte[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int bytesRemaining = length;
            while (bytesRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, bytesRemaining));
                int bufferCapacity = buffer.remainingCapacity();
                int bytesToCopy = min(bytesRemaining, bufferCapacity);
                buffer.writeBytes(source, currentIndex, bytesToCopy);
                currentIndex += bytesToCopy;
                bytesRemaining -= bytesToCopy;
            }
            uncompressedSize += length;
        }

        @Override
        public void writeShorts(short[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int shortsRemaining = length;
            while (shortsRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, shortsRemaining * Short.BYTES));
                int bufferCapacity = buffer.remainingCapacity();
                int shortsToCopy = min(shortsRemaining, bufferCapacity / Short.BYTES);
                buffer.writeShorts(source, currentIndex, shortsToCopy);
                currentIndex += shortsToCopy;
                shortsRemaining -= shortsToCopy;
            }
            uncompressedSize += length * Short.BYTES;
        }

        @Override
        public void writeInts(int[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int intsRemaining = length;
            while (intsRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, intsRemaining * Integer.BYTES));
                int bufferCapacity = buffer.remainingCapacity();
                int intsToCopy = min(intsRemaining, bufferCapacity / Integer.BYTES);
                buffer.writeInts(source, currentIndex, intsToCopy);
                currentIndex += intsToCopy;
                intsRemaining -= intsToCopy;
            }
            uncompressedSize += length * Integer.BYTES;
        }

        @Override
        public void writeLongs(long[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int longsRemaining = length;
            while (longsRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, longsRemaining * Long.BYTES));
                int bufferCapacity = buffer.remainingCapacity();
                int longsToCopy = min(longsRemaining, bufferCapacity / Long.BYTES);
                buffer.writeLongs(source, currentIndex, longsToCopy);
                currentIndex += longsToCopy;
                longsRemaining -= longsToCopy;
            }
            uncompressedSize += length * Long.BYTES;
        }

        @Override
        public void writeFloats(float[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int floatsRemaining = length;
            while (floatsRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, floatsRemaining * Float.BYTES));
                int bufferCapacity = buffer.remainingCapacity();
                int floatsToCopy = min(floatsRemaining, bufferCapacity / Float.BYTES);
                buffer.writeFloats(source, currentIndex, floatsToCopy);
                currentIndex += floatsToCopy;
                floatsRemaining -= floatsToCopy;
            }
            uncompressedSize += length * Float.BYTES;
        }

        @Override
        public void writeDoubles(double[] source, int sourceIndex, int length)
        {
            WriteBuffer buffer = buffers[0];
            int currentIndex = sourceIndex;
            int doublesRemaining = length;
            while (doublesRemaining > 0) {
                ensureCapacityFor(min(Long.BYTES, doublesRemaining * Double.BYTES));
                int bufferCapacity = buffer.remainingCapacity();
                int doublesToCopy = min(doublesRemaining, bufferCapacity / Double.BYTES);
                buffer.writeDoubles(source, currentIndex, doublesToCopy);
                currentIndex += doublesToCopy;
                doublesRemaining -= doublesToCopy;
            }
            uncompressedSize += length * Double.BYTES;
        }

        public Slice closePage()
        {
            compress();
            encrypt();

            WriteBuffer pageBuffer = buffers[buffers.length - 1];
            int serializedPageSize = pageBuffer.getPosition();
            int compressedSize = serializedPageSize - SERIALIZED_PAGE_HEADER_SIZE;
            Slice slice = pageBuffer.getSlice();
            slice.setInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET, uncompressedSize);
            slice.setInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET, compressedSize);

            Slice page;
            if (serializedPageSize < slice.length() / 2) {
                page = slice.copy(0, serializedPageSize);
            }
            else {
                page = slice.slice(0, serializedPageSize);
            }
            for (WriteBuffer buffer : buffers) {
                buffer.reset();
            }
            buffers[buffers.length - 1] = null;
            uncompressedSize = 0;
            return page;
        }

        private void ensureCapacityFor(int bytes)
        {
            if (buffers[0].remainingCapacity() >= bytes) {
                return;
            }
            // expand page output buffer
            buffers[buffers.length - 1].ensureCapacityFor(bytes);

            compress();
            encrypt();
        }

        private void compress()
        {
            if (this.compressor.isEmpty()) {
                return;
            }
            Compressor compressor = this.compressor.get();

            WriteBuffer sourceBuffer = buffers[0];
            WriteBuffer sinkBuffer = buffers[1];

            int maxCompressedLength = compressor.maxCompressedLength(sourceBuffer.getPosition());
            sinkBuffer.ensureCapacityFor(maxCompressedLength + Integer.BYTES);

            int uncompressedSize = sourceBuffer.getPosition();
            int compressedSize = compressor.compress(
                    sourceBuffer.getSlice().byteArray(),
                    sourceBuffer.getSlice().byteArrayOffset(),
                    uncompressedSize,
                    sinkBuffer.getSlice().byteArray(),
                    sinkBuffer.getSlice().byteArrayOffset() + sinkBuffer.getPosition() + Integer.BYTES,
                    maxCompressedLength);

            boolean compressed = uncompressedSize * MINIMUM_COMPRESSION_RATIO > compressedSize;
            int blockSize;
            if (!compressed) {
                System.arraycopy(
                        sourceBuffer.getSlice().byteArray(),
                        sourceBuffer.getSlice().byteArrayOffset(),
                        sinkBuffer.getSlice().byteArray(),
                        sinkBuffer.getSlice().byteArrayOffset() + sinkBuffer.getPosition() + Integer.BYTES,
                        uncompressedSize);
                blockSize = uncompressedSize;
            }
            else {
                blockSize = compressedSize;
            }

            sinkBuffer.writeInt(createBlockMarker(compressed, blockSize));
            sinkBuffer.skip(blockSize);

            sourceBuffer.reset();
        }

        private static int createBlockMarker(boolean compressed, int size)
        {
            if (compressed) {
                return size | SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK;
            }
            return size;
        }

        private void encrypt()
        {
            if (encryptionKey.isEmpty()) {
                return;
            }
            Cipher cipher = initCipher(encryptionKey.get());
            byte[] iv = cipher.getIV();

            WriteBuffer sourceBuffer = buffers[buffers.length - 2];
            WriteBuffer sinkBuffer = buffers[buffers.length - 1];

            int maxEncryptedSize = cipher.getOutputSize(sourceBuffer.getPosition()) + iv.length;
            sinkBuffer.ensureCapacityFor(maxEncryptedSize
                    // to store encrypted block length
                    + Integer.BYTES
                    // to store initialization vector
                    + iv.length);
            // reserve space for encrypted block length
            sinkBuffer.skip(Integer.BYTES);
            // write initialization vector
            sinkBuffer.writeBytes(iv, 0, iv.length);

            int encryptedSize;
            try {
                // Do not refactor into single doFinal call, performance and allocation rate are significantly worse
                // See https://github.com/trinodb/trino/pull/5557
                encryptedSize = cipher.update(
                        sourceBuffer.getSlice().byteArray(),
                        sourceBuffer.getSlice().byteArrayOffset(),
                        sourceBuffer.getPosition(),
                        sinkBuffer.getSlice().byteArray(),
                        sinkBuffer.getSlice().byteArrayOffset() + sinkBuffer.getPosition());
                encryptedSize += cipher.doFinal(
                        sinkBuffer.getSlice().byteArray(),
                        sinkBuffer.getSlice().byteArrayOffset() + sinkBuffer.getPosition() + encryptedSize);
            }
            catch (GeneralSecurityException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encrypt data: " + e.getMessage(), e);
            }

            sinkBuffer.getSlice().setInt(sinkBuffer.getPosition() - Integer.BYTES - iv.length, encryptedSize);
            sinkBuffer.skip(encryptedSize);

            sourceBuffer.reset();
        }

        private Cipher initCipher(SecretKey key)
        {
            Cipher cipher = this.cipher.orElseThrow(() -> new VerifyException("cipher is expected to be present"));
            try {
                cipher.init(ENCRYPT_MODE, key);
            }
            catch (GeneralSecurityException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to init cipher: " + e.getMessage(), e);
            }
            return cipher;
        }

        @Override
        public long getRetainedSize()
        {
            long size = INSTANCE_SIZE;
            size += sizeOf(compressor, compressor -> COMPRESSOR_RETAINED_SIZE);
            size += sizeOf(encryptionKey, encryptionKey -> ENCRYPTION_KEY_RETAINED_SIZE);
            size += sizeOf(cipher, cipher -> ESTIMATED_AES_CIPHER_RETAINED_SIZE);
            for (WriteBuffer buffer : buffers) {
                if (buffer != null) {
                    size += buffer.getRetainedSizeInBytes();
                }
            }
            return size;
        }

        @Override
        public int writableBytes()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isWritable()
        {
            return true;
        }

        @Override
        public void writeBytes(byte[] source)
        {
            writeBytes(source, 0, source.length);
        }

        @Override
        public void writeBytes(Slice source)
        {
            writeBytes(source, 0, source.length());
        }

        @Override
        public void writeBytes(InputStream in, int length)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice slice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getUnderlyingSlice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset(int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString(Charset charset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SliceOutput appendLong(long value)
        {
            writeLong(value);
            return this;
        }

        @Override
        public SliceOutput appendDouble(double value)
        {
            writeDouble(value);
            return this;
        }

        @Override
        public SliceOutput appendInt(int value)
        {
            writeInt(value);
            return this;
        }

        @Override
        public SliceOutput appendShort(int value)
        {
            writeShort(value);
            return this;
        }

        @Override
        public SliceOutput appendByte(int value)
        {
            writeByte(value);
            return this;
        }

        @Override
        public SliceOutput appendBytes(byte[] source, int sourceIndex, int length)
        {
            writeBytes(source, sourceIndex, length);
            return this;
        }

        @Override
        public SliceOutput appendBytes(byte[] source)
        {
            return appendBytes(source, 0, source.length);
        }

        @Override
        public SliceOutput appendBytes(Slice slice)
        {
            writeBytes(slice);
            return this;
        }
    }

    private static class WriteBuffer
    {
        private static final int INSTANCE_SIZE = instanceSize(WriteBuffer.class);

        private Slice slice;
        private int position;

        public WriteBuffer(int initialCapacity)
        {
            this.slice = Slices.allocate(initialCapacity);
        }

        public void writeByte(int value)
        {
            slice.setByte(position, value);
            position += Byte.BYTES;
        }

        public void writeShort(int value)
        {
            slice.setShort(position, value);
            position += Short.BYTES;
        }

        public void writeInt(int value)
        {
            slice.setInt(position, value);
            position += Integer.BYTES;
        }

        public void writeLong(long value)
        {
            slice.setLong(position, value);
            position += Long.BYTES;
        }

        public void writeFloat(float value)
        {
            slice.setFloat(position, value);
            position += Float.BYTES;
        }

        public void writeDouble(double value)
        {
            slice.setDouble(position, value);
            position += Double.BYTES;
        }

        public void writeBytes(Slice source, int sourceIndex, int length)
        {
            slice.setBytes(position, source, sourceIndex, length);
            position += length;
        }

        public void writeBytes(byte[] source, int sourceIndex, int length)
        {
            slice.setBytes(position, source, sourceIndex, length);
            position += length;
        }

        public void writeShorts(short[] source, int sourceIndex, int length)
        {
            slice.setShorts(position, source, sourceIndex, length);
            position += length * Short.BYTES;
        }

        public void writeInts(int[] source, int sourceIndex, int length)
        {
            slice.setInts(position, source, sourceIndex, length);
            position += length * Integer.BYTES;
        }

        public void writeLongs(long[] source, int sourceIndex, int length)
        {
            slice.setLongs(position, source, sourceIndex, length);
            position += length * Long.BYTES;
        }

        public void writeFloats(float[] source, int sourceIndex, int length)
        {
            slice.setFloats(position, source, sourceIndex, length);
            position += length * Float.BYTES;
        }

        public void writeDoubles(double[] source, int sourceIndex, int length)
        {
            slice.setDoubles(position, source, sourceIndex, length);
            position += length * Double.BYTES;
        }

        public void skip(int length)
        {
            position += length;
        }

        public int remainingCapacity()
        {
            return slice.length() - position;
        }

        public int getPosition()
        {
            return position;
        }

        public Slice getSlice()
        {
            return slice;
        }

        public void reset()
        {
            position = 0;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + slice.getRetainedSize();
        }

        public void ensureCapacityFor(int bytes)
        {
            slice = Slices.ensureSize(slice, position + bytes);
        }
    }
}
