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
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.execution.buffer.PagesSerdeUtil.ESTIMATED_AES_CIPHER_RETAINED_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_CIPHER_NAME;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PagesSerdeUtil.readRawPage;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;

public class CompressingDecryptingPageDeserializer
        implements PageDeserializer
{
    private static final int INSTANCE_SIZE = instanceSize(CompressingDecryptingPageDeserializer.class);

    private final BlockEncodingSerde blockEncodingSerde;
    private final SerializedPageInput input;

    public CompressingDecryptingPageDeserializer(
            BlockEncodingSerde blockEncodingSerde,
            Optional<Decompressor> decompressor,
            Optional<SecretKey> encryptionKey,
            int blockSizeInBytes,
            OptionalInt maxCompressedBlockSizeInBytes)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
        encryptionKey.ifPresent(secretKey -> checkArgument(is256BitSecretKeySpec(secretKey), "encryptionKey is expected to be an instance of SecretKeySpec containing a 256bit key"));
        input = new SerializedPageInput(
                requireNonNull(decompressor, "decompressor is null"),
                encryptionKey,
                blockSizeInBytes,
                maxCompressedBlockSizeInBytes);
    }

    @Override
    public Page deserialize(Slice serializedPage)
    {
        int positionCount = input.startPage(serializedPage);
        Page page = readRawPage(positionCount, input, blockEncodingSerde);
        input.finishPage();
        return page;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + input.getRetainedSize();
    }

    private static class SerializedPageInput
            extends SliceInput
    {
        private static final int INSTANCE_SIZE = instanceSize(SerializedPageInput.class);
        // TODO: implement getRetainedSizeInBytes in Lz4Decompressor
        private static final int DECOMPRESSOR_RETAINED_SIZE = instanceSize(Lz4Decompressor.class);
        private static final int ENCRYPTION_KEY_RETAINED_SIZE = toIntExact(instanceSize(SecretKeySpec.class) + sizeOfByteArray(256 / 8));

        private final Optional<Decompressor> decompressor;
        private final Optional<SecretKey> encryptionKey;
        private final Optional<Cipher> cipher;

        private final ReadBuffer[] buffers;

        private SerializedPageInput(Optional<Decompressor> decompressor, Optional<SecretKey> encryptionKey, int blockSizeInBytes, OptionalInt maxCompressedBlockSizeInBytes)
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
                    // to store compressed block size
                    bufferSize = maxCompressedBlockSizeInBytes.orElseThrow()
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
                    cipher = Optional.of(Cipher.getInstance(SERIALIZED_PAGE_CIPHER_NAME));
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
        public void readShorts(short[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int shortsRemaining = length;
            while (shortsRemaining > 0) {
                ensureReadable(min(Long.BYTES, shortsRemaining * Short.BYTES));
                int shortsToRead = min(shortsRemaining, buffer.available() / Short.BYTES);
                buffer.readShorts(destination, destinationIndex, shortsToRead);
                shortsRemaining -= shortsToRead;
                destinationIndex += shortsToRead;
            }
        }

        @Override
        public void readInts(int[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int intsRemaining = length;
            while (intsRemaining > 0) {
                ensureReadable(min(Long.BYTES, intsRemaining * Integer.BYTES));
                int intsToRead = min(intsRemaining, buffer.available() / Integer.BYTES);
                buffer.readInts(destination, destinationIndex, intsToRead);
                intsRemaining -= intsToRead;
                destinationIndex += intsToRead;
            }
        }

        @Override
        public void readLongs(long[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int longsRemaining = length;
            while (longsRemaining > 0) {
                ensureReadable(min(Long.BYTES, longsRemaining * Long.BYTES));
                int longsToRead = min(longsRemaining, buffer.available() / Long.BYTES);
                buffer.readLongs(destination, destinationIndex, longsToRead);
                longsRemaining -= longsToRead;
                destinationIndex += longsToRead;
            }
        }

        @Override
        public void readFloats(float[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int floatsRemaining = length;
            while (floatsRemaining > 0) {
                ensureReadable(min(Long.BYTES, floatsRemaining * Float.BYTES));
                int floatsToRead = min(floatsRemaining, buffer.available() / Float.BYTES);
                buffer.readFloats(destination, destinationIndex, floatsToRead);
                floatsRemaining -= floatsToRead;
                destinationIndex += floatsToRead;
            }
        }

        @Override
        public void readDoubles(double[] destination, int destinationIndex, int length)
        {
            ReadBuffer buffer = buffers[0];
            int doublesRemaining = length;
            while (doublesRemaining > 0) {
                ensureReadable(min(Long.BYTES, doublesRemaining * Double.BYTES));
                int doublesToRead = min(doublesRemaining, buffer.available() / Double.BYTES);
                buffer.readDoubles(destination, destinationIndex, doublesToRead);
                doublesRemaining -= doublesToRead;
                destinationIndex += doublesToRead;
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
                        sink.getSlice().length() - bytesPreserved);
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
            return compressedBlockMarker & ~SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK;
        }

        private static boolean isCompressed(int compressedBlockMarker)
        {
            return (compressedBlockMarker & SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK) == SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK;
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
            size += sizeOf(decompressor, compressor -> DECOMPRESSOR_RETAINED_SIZE);
            size += sizeOf(encryptionKey, encryptionKey -> ENCRYPTION_KEY_RETAINED_SIZE);
            size += sizeOf(cipher, cipher -> ESTIMATED_AES_CIPHER_RETAINED_SIZE);
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
        private static final int INSTANCE_SIZE = instanceSize(ReadBuffer.class);

        private final Slice slice;
        private int position;
        private int limit;

        public ReadBuffer(Slice slice)
        {
            requireNonNull(slice, "slice is null");
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

        public void readShorts(short[] destination, int destinationIndex, int length)
        {
            slice.getShorts(position, destination, destinationIndex, length);
            position += length * Short.BYTES;
        }

        public void readInts(int[] destination, int destinationIndex, int length)
        {
            slice.getInts(position, destination, destinationIndex, length);
            position += length * Integer.BYTES;
        }

        public void readLongs(long[] destination, int destinationIndex, int length)
        {
            slice.getLongs(position, destination, destinationIndex, length);
            position += length * Long.BYTES;
        }

        public void readFloats(float[] destination, int destinationIndex, int length)
        {
            slice.getFloats(position, destination, destinationIndex, length);
            position += length * Float.BYTES;
        }

        public void readDoubles(double[] destination, int destinationIndex, int length)
        {
            slice.getDoubles(position, destination, destinationIndex, length);
            position += length * Double.BYTES;
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
