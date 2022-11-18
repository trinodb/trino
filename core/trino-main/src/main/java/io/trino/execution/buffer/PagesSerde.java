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
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.execution.buffer.PageCodecMarker.MarkerSet;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.ByteStreams.readFully;
import static io.airlift.slice.UnsafeSlice.getIntUnchecked;
import static io.trino.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.trino.execution.buffer.PageCodecMarker.ENCRYPTED;
import static io.trino.execution.buffer.PagesSerdeUtil.readRawPage;
import static io.trino.execution.buffer.PagesSerdeUtil.writeRawPage;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

@NotThreadSafe
public class PagesSerde
{
    private static final int SERIALIZED_PAGE_POSITION_COUNT_OFFSET = 0;
    private static final int SERIALIZED_PAGE_CODEC_MARKERS_OFFSET = SERIALIZED_PAGE_POSITION_COUNT_OFFSET + Integer.BYTES;
    private static final int SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET = SERIALIZED_PAGE_CODEC_MARKERS_OFFSET + Byte.BYTES;
    private static final int SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET = SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET + Integer.BYTES;
    static final int SERIALIZED_PAGE_HEADER_SIZE = SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET + Integer.BYTES;
    private static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 64 * 1024;
    private static final String CIPHER_NAME = "AES/CBC/PKCS5Padding";
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;
    private static final int COMPRESSED_BLOCK_MASK = 1 << (Integer.SIZE - 1);

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<Compressor> compressor;
    private final Optional<Decompressor> decompressor;
    private final Optional<SecretKey> encryptionKey;
    private final int blockSizeInBytes;

    private SerializedPageOutput output;
    private SerializedPageInput input;

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, Optional<SecretKey> encryptionKey)
    {
        this(blockEncodingSerde, compressionEnabled, encryptionKey, DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, Optional<SecretKey> encryptionKey, int blockSizeInBytes)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        if (compressionEnabled) {
            compressor = Optional.of(new Lz4Compressor());
            decompressor = Optional.of(new Lz4Decompressor());
        }
        else {
            compressor = Optional.empty();
            decompressor = Optional.empty();
        }
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        this.blockSizeInBytes = blockSizeInBytes;
    }

    public Slice serialize(Page page)
    {
        if (output == null) {
            output = new SerializedPageOutput(compressor, encryptionKey, blockSizeInBytes);
        }
        output.startPage(page.getPositionCount(), toIntExact(page.getSizeInBytes()));
        writeRawPage(page, output, blockEncodingSerde);
        return output.closePage();
    }

    public static int getSerializedPagePositionCount(Slice serializedPage)
    {
        return serializedPage.getInt(SERIALIZED_PAGE_POSITION_COUNT_OFFSET);
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
        if (input == null) {
            input = new SerializedPageInput(decompressor, compressor, encryptionKey, blockSizeInBytes);
        }
        int positionCount = input.startPage(serializedPage);
        Page page = readRawPage(positionCount, input, blockEncodingSerde);
        input.finishPage();
        return page;
    }

    public static Slice readSerializedPage(Slice headerSlice, InputStream inputStream)
            throws IOException
    {
        checkArgument(headerSlice.length() == SERIALIZED_PAGE_HEADER_SIZE, "headerSlice length should equal to %s", SERIALIZED_PAGE_HEADER_SIZE);

        int compressedSize = getIntUnchecked(headerSlice, SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET);
        byte[] outputBuffer = new byte[SERIALIZED_PAGE_HEADER_SIZE + compressedSize];
        headerSlice.getBytes(0, outputBuffer, 0, SERIALIZED_PAGE_HEADER_SIZE);
        readFully(inputStream, outputBuffer, SERIALIZED_PAGE_HEADER_SIZE, compressedSize);
        return Slices.wrappedBuffer(outputBuffer);
    }

    private static class SerializedPageOutput
            extends SliceOutput
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SerializedPageOutput.class).instanceSize());

        private final Optional<Compressor> compressor;
        private final Optional<SecretKey> encryptionKey;
        private final int markers;
        private final Optional<Cipher> cipher;

        private final WriteBuffer[] buffers;
        private int uncompressedSize;

        private SerializedPageOutput(
                Optional<Compressor> compressor,
                Optional<SecretKey> encryptionKey,
                int blockSizeInBytes)
        {
            this.compressor = requireNonNull(compressor, "compressor is null");
            this.encryptionKey = requireNonNull(encryptionKey, "cipher is null");

            buffers = new WriteBuffer[
                    (compressor.isPresent() ? 1 : 0) // compression buffer
                            + (encryptionKey.isPresent() ? 1 : 0) // encryption buffer
                            + 1 // output buffer
                    ];
            MarkerSet markerSet = MarkerSet.empty();
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
                    cipher = Optional.of(Cipher.getInstance(CIPHER_NAME));
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
                page = Slices.copyOf(slice, 0, serializedPageSize);
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
                return size | COMPRESSED_BLOCK_MASK;
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
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(WriteBuffer.class).instanceSize());

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

    private static class SerializedPageInput
            extends SliceInput
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SerializedPageInput.class).instanceSize());

        private final Optional<Decompressor> decompressor;
        private final Optional<SecretKey> encryptionKey;
        private final Optional<Cipher> cipher;

        private final ReadBuffer[] buffers;

        private SerializedPageInput(Optional<Decompressor> decompressor, Optional<Compressor> compressor, Optional<SecretKey> encryptionKey, int blockSizeInBytes)
        {
            this.decompressor = requireNonNull(decompressor, "decompressor is null");
            this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");

            buffers = new ReadBuffer[
                    (decompressor.isPresent() ? 1 : 0) // decompression buffer
                            + (encryptionKey.isPresent() ? 1 : 0) // decryption buffer
                            + 1 // input buffer
                    ];
            if (decompressor.isPresent()) {
                int bufferSize = blockSizeInBytes
                        // to guarantee a single long can always be read entirely
                        + Long.BYTES;
                buffers[0] = new ReadBuffer(Slices.allocate(bufferSize));
                buffers[0].setPosition(bufferSize);
            }
            if (encryptionKey.isPresent()) {
                int bufferSize;
                if (decompressor.isPresent()) {
                    verify(compressor.isPresent());
                    // to store compressed block size
                    bufferSize = compressor.get().maxCompressedLength(blockSizeInBytes)
                            // to store compressed block size
                            + Integer.BYTES
                            // to guarantee a single long can always be read entirely
                            + Long.BYTES;
                }
                else {
                    bufferSize = blockSizeInBytes
                            // to guarantee a single long can always be read entirely
                            + Long.BYTES;
                }
                buffers[buffers.length - 2] = new ReadBuffer(Slices.allocate(bufferSize));
                buffers[buffers.length - 2].setPosition(bufferSize);

                try {
                    cipher = Optional.of(Cipher.getInstance(CIPHER_NAME));
                }
                catch (GeneralSecurityException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create cipher: " + e.getMessage(), e);
                }
            }
            else {
                cipher = Optional.empty();
            }
        }

        public int startPage(Slice page)
        {
            int positionCount = getSerializedPagePositionCount(page);
            ReadBuffer buffer = new ReadBuffer(page);
            buffer.setPosition(SERIALIZED_PAGE_HEADER_SIZE);
            buffers[buffers.length - 1] = buffer;
            return positionCount;
        }

        @Override
        public boolean readBoolean()
        {
            ensureReadable(1);
            return buffers[0].readBoolean();
        }

        @Override
        public byte readByte()
        {
            ensureReadable(Byte.BYTES);
            return buffers[0].readByte();
        }

        @Override
        public short readShort()
        {
            ensureReadable(Short.BYTES);
            return buffers[0].readShort();
        }

        @Override
        public int readInt()
        {
            ensureReadable(Integer.BYTES);
            return buffers[0].readInt();
        }

        @Override
        public long readLong()
        {
            ensureReadable(Long.BYTES);
            return buffers[0].readLong();
        }

        @Override
        public float readFloat()
        {
            ensureReadable(Float.BYTES);
            return buffers[0].readFloat();
        }

        @Override
        public double readDouble()
        {
            ensureReadable(Double.BYTES);
            return buffers[0].readDouble();
        }

        @Override
        public int read(byte[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int bytesRemaining = length;
            while (bytesRemaining > 0) {
                ensureReadable(min(Long.BYTES, bytesRemaining));
                int bytesToRead = min(bytesRemaining, buffer.available());
                int bytesRead = buffer.read(destination, destinationIndex, bytesToRead);
                if (bytesRead == -1) {
                    break;
                }
                bytesRemaining -= bytesRead;
                destinationIndex += bytesRead;
            }
            return length - bytesRemaining;
        }

        @Override
        public void readBytes(byte[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int bytesRemaining = length;
            while (bytesRemaining > 0) {
                ensureReadable(min(Long.BYTES, bytesRemaining));
                int bytesToRead = min(bytesRemaining, buffer.available());
                buffer.readBytes(destination, destinationIndex, bytesToRead);
                bytesRemaining -= bytesToRead;
                destinationIndex += bytesToRead;
            }
        }

        @Override
        public void readBytes(Slice destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int bytesRemaining = length;
            while (bytesRemaining > 0) {
                ensureReadable(min(Long.BYTES, bytesRemaining));
                int bytesToRead = min(bytesRemaining, buffer.available());
                buffer.readBytes(destination, destinationIndex, bytesToRead);
                bytesRemaining -= bytesToRead;
                destinationIndex += bytesToRead;
            }
        }

        private void ensureReadable(int bytes)
        {
            if (buffers[0].available() >= bytes) {
                return;
            }
            decrypt();
            decompress();
        }

        private void decrypt()
        {
            if (this.encryptionKey.isEmpty()) {
                return;
            }

            ReadBuffer source = buffers[buffers.length - 1];
            ReadBuffer sink = buffers[buffers.length - 2];
            int bytesPreserved = sink.rollOver();

            int encryptedSize = source.readInt();
            int ivSize = cipher.orElseThrow().getBlockSize();
            IvParameterSpec iv = new IvParameterSpec(
                    source.getSlice().byteArray(),
                    source.getSlice().byteArrayOffset() + source.getPosition(),
                    ivSize);
            source.setPosition(source.getPosition() + ivSize);

            Cipher cipher = initCipher(encryptionKey.get(), iv);
            int decryptedSize;
            try {
                // Do not refactor into single doFinal call, performance and allocation rate are significantly worse
                // See https://github.com/trinodb/trino/pull/5557
                decryptedSize = cipher.update(
                        source.getSlice().byteArray(),
                        source.getSlice().byteArrayOffset() + source.getPosition(),
                        encryptedSize,
                        sink.getSlice().byteArray(),
                        sink.getSlice().byteArrayOffset() + bytesPreserved);
                decryptedSize += cipher.doFinal(
                        sink.getSlice().byteArray(),
                        sink.getSlice().byteArrayOffset() + bytesPreserved + decryptedSize);
            }
            catch (GeneralSecurityException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot decrypt previously encrypted data: " + e.getMessage(), e);
            }
            source.setPosition(source.getPosition() + encryptedSize);
            sink.setLimit(bytesPreserved + decryptedSize);
        }

        private Cipher initCipher(SecretKey key, IvParameterSpec iv)
        {
            Cipher cipher = this.cipher.orElseThrow(() -> new VerifyException("cipher is expected to be present"));
            try {
                cipher.init(DECRYPT_MODE, key, iv);
            }
            catch (GeneralSecurityException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to init cipher: " + e.getMessage(), e);
            }
            return cipher;
        }

        private void decompress()
        {
            if (this.decompressor.isEmpty()) {
                return;
            }

            Decompressor decompressor = this.decompressor.get();

            ReadBuffer source = buffers[1];
            ReadBuffer sink = buffers[0];
            int bytesPreserved = sink.rollOver();

            int compressedBlockMarker = source.readInt();
            int blockSize = getCompressedBlockSize(compressedBlockMarker);
            boolean compressed = isCompressed(compressedBlockMarker);

            int decompressedSize;
            if (compressed) {
                decompressedSize = decompressor.decompress(
                        source.getSlice().byteArray(),
                        source.getSlice().byteArrayOffset() + source.getPosition(),
                        blockSize,
                        sink.getSlice().byteArray(),
                        sink.getSlice().byteArrayOffset() + bytesPreserved,
                        sink.getSlice().length());
            }
            else {
                System.arraycopy(
                        source.getSlice().byteArray(),
                        source.getSlice().byteArrayOffset() + source.getPosition(),
                        sink.getSlice().byteArray(),
                        sink.getSlice().byteArrayOffset() + bytesPreserved,
                        blockSize);
                decompressedSize = blockSize;
            }
            source.setPosition(source.getPosition() + blockSize);
            sink.setLimit(bytesPreserved + decompressedSize);
        }

        private static int getCompressedBlockSize(int compressedBlockMarker)
        {
            return compressedBlockMarker & (~COMPRESSED_BLOCK_MASK);
        }

        private static boolean isCompressed(int compressedBlockMarker)
        {
            return (compressedBlockMarker & COMPRESSED_BLOCK_MASK) == COMPRESSED_BLOCK_MASK;
        }

        public void finishPage()
        {
            buffers[buffers.length - 1] = null;
            for (ReadBuffer buffer : buffers) {
                if (buffer != null) {
                    buffer.setPosition(buffer.getSlice().length());
                    buffer.setLimit(buffer.getSlice().length());
                }
            }
        }

        @Override
        public int read()
        {
            return readByte();
        }

        @Override
        public int readUnsignedByte()
        {
            return readByte() & 0xFF;
        }

        @Override
        public int readUnsignedShort()
        {
            return readShort() & 0xFFFF;
        }

        @Override
        public Slice readSlice(int length)
        {
            Slice slice = Slices.allocate(length);
            readBytes(slice, 0, length);
            return slice;
        }

        @Override
        public boolean isReadable()
        {
            return available() > 0;
        }

        @Override
        public int available()
        {
            return buffers[0].available();
        }

        @Override
        public long skip(long length)
        {
            return 0;
        }

        @Override
        public int skipBytes(int length)
        {
            return toIntExact(skip(length));
        }

        @Override
        public long getRetainedSize()
        {
            long size = INSTANCE_SIZE;
            for (ReadBuffer input : buffers) {
                if (input != null) {
                    size += input.getRetainedSizeInBytes();
                }
            }
            return size;
        }

        @Override
        public void readBytes(OutputStream out, int length)
                throws IOException
        {
            throw new UnsupportedEncodingException();
        }

        @Override
        public long position()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPosition(long position)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class ReadBuffer
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(ReadBuffer.class).instanceSize());

        private final Slice slice;
        private int position;
        private int limit;

        public ReadBuffer(Slice slice)
        {
            requireNonNull(slice, "slice is null");
            checkArgument(slice.hasByteArray(), "slice is expected to be based on a byte array");
            this.slice = slice;
            limit = slice.length();
        }

        public int available()
        {
            return limit - position;
        }

        public Slice getSlice()
        {
            return slice;
        }

        public int getPosition()
        {
            return position;
        }

        public void setPosition(int position)
        {
            this.position = position;
        }

        public void setLimit(int limit)
        {
            this.limit = limit;
        }

        public int rollOver()
        {
            int bytesToCopy = available();
            if (bytesToCopy != 0) {
                slice.setBytes(0, slice, position, bytesToCopy);
            }
            position = 0;
            return bytesToCopy;
        }

        public boolean readBoolean()
        {
            boolean value = slice.getByte(position) == 1;
            position += Byte.BYTES;
            return value;
        }

        public byte readByte()
        {
            byte value = slice.getByte(position);
            position += Byte.BYTES;
            return value;
        }

        public short readShort()
        {
            short value = slice.getShort(position);
            position += Short.BYTES;
            return value;
        }

        public int readInt()
        {
            int value = slice.getInt(position);
            position += Integer.BYTES;
            return value;
        }

        public long readLong()
        {
            long value = slice.getLong(position);
            position += Long.BYTES;
            return value;
        }

        public float readFloat()
        {
            float value = slice.getFloat(position);
            position += Float.BYTES;
            return value;
        }

        public double readDouble()
        {
            double value = slice.getDouble(position);
            position += Double.BYTES;
            return value;
        }

        public int read(byte[] destination, int destinationIndex, int length)
        {
            int bytesToRead = min(length, slice.length() - position);
            slice.getBytes(position, destination, destinationIndex, bytesToRead);
            position += bytesToRead;
            return bytesToRead;
        }

        public void readBytes(byte[] destination, int destinationIndex, int length)
        {
            slice.getBytes(position, destination, destinationIndex, length);
            position += length;
        }

        public void readBytes(Slice destination, int destinationIndex, int length)
        {
            slice.getBytes(position, destination, destinationIndex, length);
            position += length;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + slice.getRetainedSize();
        }
    }
}
