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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BitPackingUtils;

import static io.trino.parquet.reader.flat.BitPackingUtils.unpack;
import static io.trino.parquet.reader.flat.VectorBitPackingUtils.vectorUnpack8FromByte;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class BooleanPlainValueDecoders
{
    private BooleanPlainValueDecoders() {}

    public static ValueDecoder<byte[]> createBooleanPlainValueDecoder(boolean vectorizedDecodingEnabled)
    {
        return vectorizedDecodingEnabled ? new VectorBooleanPlainValueDecoder() : new BooleanPlainValueDecoder();
    }

    private abstract static class AbstractBooleanPlainValueDecoder
            implements ValueDecoder<byte[]>
    {
        protected SimpleSliceInputStream input;
        // Number of unread bits in the current byte
        protected int alreadyReadBits;
        // Partly read byte
        protected byte partiallyReadByte;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
            alreadyReadBits = 0;
        }

        @Override
        public void skip(int n)
        {
            if (alreadyReadBits != 0) { // Skip the partially read byte
                int chunkSize = min(Byte.SIZE - alreadyReadBits, n);
                n -= chunkSize;
                alreadyReadBits = (alreadyReadBits + chunkSize) % Byte.SIZE; // Set to 0 when full byte reached
            }

            // Skip full bytes
            input.skip(n / Byte.SIZE);

            if (n % Byte.SIZE != 0) { // Partially skip the last byte
                alreadyReadBits = n % Byte.SIZE;
                partiallyReadByte = input.readByte();
            }
        }
    }

    private static final class BooleanPlainValueDecoder
            extends AbstractBooleanPlainValueDecoder
    {
        @Override
        public void read(byte[] values, int offset, int length)
        {
            if (alreadyReadBits != 0) { // Use partially unpacked byte
                int bitsRemaining = Byte.SIZE - alreadyReadBits;
                int chunkSize = min(bitsRemaining, length);
                unpack(values, offset, partiallyReadByte, alreadyReadBits, alreadyReadBits + chunkSize);
                alreadyReadBits = (alreadyReadBits + chunkSize) % Byte.SIZE; // Set to 0 when full byte reached
                if (length == chunkSize) {
                    return;
                }
                offset += chunkSize;
                length -= chunkSize;
            }

            // Read full bytes
            int bytesToRead = length / Byte.SIZE;
            while (bytesToRead > 0) {
                byte packedByte = input.readByte();
                BitPackingUtils.unpack8FromByte(values, offset, packedByte);
                bytesToRead--;
                offset += Byte.SIZE;
            }

            // Partially read the last byte
            alreadyReadBits = length % Byte.SIZE;
            if (alreadyReadBits != 0) {
                partiallyReadByte = input.readByte();
                unpack(values, offset, partiallyReadByte, 0, alreadyReadBits);
            }
        }
    }

    private static final class VectorBooleanPlainValueDecoder
            extends AbstractBooleanPlainValueDecoder
    {
        @Override
        public void read(byte[] values, int offset, int length)
        {
            if (alreadyReadBits != 0) { // Use partially unpacked byte
                int bitsRemaining = Byte.SIZE - alreadyReadBits;
                int chunkSize = min(bitsRemaining, length);
                unpack(values, offset, partiallyReadByte, alreadyReadBits, alreadyReadBits + chunkSize);
                alreadyReadBits = (alreadyReadBits + chunkSize) % Byte.SIZE; // Set to 0 when full byte reached
                if (length == chunkSize) {
                    return;
                }
                offset += chunkSize;
                length -= chunkSize;
            }

            // Read full bytes
            int bytesToRead = length / Byte.SIZE;
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (inputBytesRead < bytesToRead) {
                vectorUnpack8FromByte(values, offset, inputArray[inputOffset + inputBytesRead]);
                offset += Byte.SIZE;
                inputBytesRead++;
            }
            input.skip(inputBytesRead);

            // Partially read the last byte
            alreadyReadBits = length % Byte.SIZE;
            if (alreadyReadBits != 0) {
                partiallyReadByte = input.readByte();
                unpack(values, offset, partiallyReadByte, 0, alreadyReadBits);
            }
        }
    }
}
