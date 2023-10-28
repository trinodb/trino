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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryInputFile;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.cycle;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
public class TestTrinoDataInputStream
{
    private static final int BUFFER_SIZE = 129;

    private static final List<Integer> VARIABLE_READ_SIZES = ImmutableList.of(
            1,
            7,
            15,
            BUFFER_SIZE - 1,
            BUFFER_SIZE,
            BUFFER_SIZE + 1,
            BUFFER_SIZE + 13);

    @Test
    public void testReadBoolean()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeBoolean(valueIndex % 2 == 0);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readBoolean()).isEqualTo(valueIndex % 2 == 0);
            }
        });
    }

    @Test
    public void testReadByte()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeByte((byte) valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readByte()).isEqualTo((byte) valueIndex);
            }
        });
    }

    @Test
    public void testRead()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_BYTE)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeByte((byte) valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.read()).isEqualTo(valueIndex & 0xFF);
            }

            @Override
            public void verifyReadOffEnd(TrinoDataInputStream input)
                    throws IOException
            {
                assertThat(input.read()).isEqualTo(-1);
            }
        });
    }

    @Test
    public void testReadShort()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_SHORT)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeShort(valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readShort()).isEqualTo((short) valueIndex);
            }
        });
    }

    @Test
    public void testReadUnsignedShort()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_SHORT)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeShort(valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readUnsignedShort()).isEqualTo(valueIndex & 0xFFF);
            }
        });
    }

    @Test
    public void testReadInt()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_INT)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeInt(valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readInt()).isEqualTo(valueIndex);
            }
        });
    }

    @Test
    public void testUnsignedReadInt()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_INT)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeInt(valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readUnsignedInt()).isEqualTo(valueIndex);
            }
        });
    }

    @Test
    public void testReadLong()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_LONG)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeLong(valueIndex);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readLong()).isEqualTo(valueIndex);
            }
        });
    }

    @Test
    public void testReadFloat()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_FLOAT)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeFloat(valueIndex + 0.12f);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readFloat()).isEqualTo(valueIndex + 0.12f);
            }
        });
    }

    @Test
    public void testReadDouble()
            throws IOException
    {
        testDataInput(new DataInputTester(SIZE_OF_DOUBLE)
        {
            @Override
            public void loadValue(DataOutputStream output, int valueIndex)
                    throws IOException
            {
                output.writeDouble(valueIndex + 0.12);
            }

            @Override
            public void verifyValue(TrinoDataInputStream input, int valueIndex)
                    throws IOException
            {
                assertThat(input.readDouble()).isEqualTo(valueIndex + 0.12);
            }
        });
    }

    @Test
    public void testSkip()
            throws IOException
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            // skip without any reads
            testDataInput(new SkipDataInputTester(readSize)
            {
                @Override
                public void verifyValue(TrinoDataInputStream input, int valueIndex)
                        throws IOException
                {
                    input.skip(valueSize());
                }

                @Override
                public void verifyReadOffEnd(TrinoDataInputStream input)
                        throws IOException
                {
                    assertThat(input.skip(valueSize())).isEqualTo(valueSize() - 1);
                }
            });
            testDataInput(new SkipDataInputTester(readSize)
            {
                @Override
                public void verifyValue(TrinoDataInputStream input, int valueIndex)
                        throws IOException
                {
                    input.skipBytes(valueSize());
                }

                @Override
                public void verifyReadOffEnd(TrinoDataInputStream input)
                        throws IOException
                {
                    assertThat(input.skip(valueSize())).isEqualTo(valueSize() - 1);
                }
            });

            // read when no data available to force buffering
            testDataInput(new SkipDataInputTester(readSize)
            {
                @Override
                public void verifyValue(TrinoDataInputStream input, int valueIndex)
                        throws IOException
                {
                    int length = valueSize();
                    while (length > 0) {
                        if (input.available() == 0) {
                            input.readByte();
                            length--;
                        }
                        int skipSize = input.skipBytes(length);
                        length -= skipSize;
                    }
                    assertThat(input.skip(0)).isEqualTo(0);
                }
            });
            testDataInput(new SkipDataInputTester(readSize)
            {
                @Override
                public void verifyValue(TrinoDataInputStream input, int valueIndex)
                        throws IOException
                {
                    long length = valueSize();
                    while (length > 0) {
                        if (input.available() == 0) {
                            input.readByte();
                            length--;
                        }
                        long skipSize = input.skip(length);
                        length -= skipSize;
                    }
                    assertThat(input.skip(0)).isEqualTo(0);
                }
            });
        }
    }

    @Test
    public void testReadSlice()
            throws IOException
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    return input.readSlice(valueSize()).toStringUtf8();
                }
            });
        }
    }

    @Test
    public void testReadFully()
            throws IOException
    {
        for (int readSize : VARIABLE_READ_SIZES) {
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    Slice slice = Slices.allocate(valueSize());
                    input.readFully(slice);
                    return slice.toStringUtf8();
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    Slice slice = Slices.allocate(valueSize() + 10);
                    input.readFully(slice, 5, valueSize());
                    return slice.slice(5, valueSize()).toStringUtf8();
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    byte[] bytes = new byte[valueSize()];
                    input.readFully(bytes, 0, valueSize());
                    return new String(bytes, 0, valueSize(), UTF_8);
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    byte[] bytes = new byte[valueSize() + 10];
                    input.readFully(bytes, 5, valueSize());
                    return new String(bytes, 5, valueSize(), UTF_8);
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    byte[] bytes = new byte[valueSize()];
                    int bytesRead = input.read(bytes);
                    if (bytesRead == -1) {
                        throw new EOFException();
                    }
                    assertThat(bytesRead)
                            .describedAs("Expected to read at least one byte")
                            .isGreaterThan(0);
                    input.readFully(bytes, bytesRead, bytes.length - bytesRead);
                    return new String(bytes, 0, valueSize(), UTF_8);
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    byte[] bytes = new byte[valueSize() + 10];
                    ByteStreams.readFully(input, bytes, 5, valueSize());
                    return new String(bytes, 5, valueSize(), UTF_8);
                }
            });
            testDataInput(new StringDataInputTester(readSize)
            {
                @Override
                public String readActual(TrinoDataInputStream input)
                        throws IOException
                {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    input.readFully(out, valueSize());
                    return out.toString(UTF_8);
                }
            });
        }
    }

    @Test
    public void testEmptyInput()
            throws Exception
    {
        TrinoDataInputStream input = createTrinoDataInputStream(new byte[0]);
        assertThat(input.getPos()).isEqualTo(0);
    }

    @Test
    public void testEmptyRead()
            throws Exception
    {
        TrinoDataInputStream input = createTrinoDataInputStream(new byte[0]);
        assertThat(input.read()).isEqualTo(-1);
    }

    @Test
    public void testReadByteBeyondEnd()
    {
        assertThatThrownBy(() -> {
            TrinoDataInputStream input = createTrinoDataInputStream(new byte[0]);
            input.readByte();
        })
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testReadShortBeyondEnd()
    {
        assertThatThrownBy(() -> {
            TrinoDataInputStream input = createTrinoDataInputStream(new byte[1]);
            input.readShort();
        })
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testReadIntBeyondEnd()
    {
        assertThatThrownBy(() -> {
            TrinoDataInputStream input = createTrinoDataInputStream(new byte[3]);
            input.readInt();
        })
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testReadLongBeyondEnd()
    {
        assertThatThrownBy(() -> {
            TrinoDataInputStream input = createTrinoDataInputStream(new byte[7]);
            input.readLong();
        })
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testEncodingBoolean()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {1}).readBoolean()).isTrue();
        assertThat(createTrinoDataInputStream(new byte[] {0}).readBoolean()).isFalse();
    }

    @Test
    public void testEncodingByte()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {92}).readByte()).isEqualTo((byte) 92);
        assertThat(createTrinoDataInputStream(new byte[] {-100}).readByte()).isEqualTo((byte) -100);
        assertThat(createTrinoDataInputStream(new byte[] {-17}).readByte()).isEqualTo((byte) -17);

        assertThat(createTrinoDataInputStream(new byte[] {92}).readUnsignedByte()).isEqualTo(92);
        assertThat(createTrinoDataInputStream(new byte[] {-100}).readUnsignedByte()).isEqualTo(156);
        assertThat(createTrinoDataInputStream(new byte[] {-17}).readUnsignedByte()).isEqualTo(239);
    }

    @Test
    public void testEncodingShort()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {109, 92}).readShort()).isEqualTo((short) 23661);
        assertThat(createTrinoDataInputStream(new byte[] {109, -100}).readShort()).isEqualTo((short) -25491);
        assertThat(createTrinoDataInputStream(new byte[] {-52, -107}).readShort()).isEqualTo((short) -27188);

        assertThat(createTrinoDataInputStream(new byte[] {109, -100}).readUnsignedShort()).isEqualTo(40045);
        assertThat(createTrinoDataInputStream(new byte[] {-52, -107}).readUnsignedShort()).isEqualTo(38348);
    }

    @Test
    public void testEncodingInteger()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {109, 92, 75, 58}).readInt()).isEqualTo(978017389);
        assertThat(createTrinoDataInputStream(new byte[] {-16, -60, -120, -1}).readInt()).isEqualTo(-7813904);
    }

    @Test
    public void testEncodingLong()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {49, -114, -96, -23, -32, -96, -32, 127}).readLong()).isEqualTo(9214541725452766769L);
        assertThat(createTrinoDataInputStream(new byte[] {109, 92, 75, 58, 18, 120, -112, -17}).readLong()).isEqualTo(-1184314682315678611L);
    }

    @Test
    public void testEncodingDouble()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {31, -123, -21, 81, -72, 30, 9, 64}).readDouble()).isEqualTo(3.14);
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, 0, 0, 0, 0, -8, 127}).readDouble()).isNaN();
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, 0, 0, 0, 0, -16, -1}).readDouble()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, 0, 0, 0, 0, -16, 127}).readDouble()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testEncodingFloat()
            throws Exception
    {
        assertThat(createTrinoDataInputStream(new byte[] {-61, -11, 72, 64}).readFloat()).isEqualTo(3.14f);
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, -64, 127}).readFloat()).isNaN();
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, -128, -1}).readFloat()).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(createTrinoDataInputStream(new byte[] {0, 0, -128, 127}).readFloat()).isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testRetainedSize()
            throws IOException
    {
        int bufferSize = 1024;
        TrinoInputStream inputStream = getMemoryInputFile(new byte[] {0, 1}).newStream();
        TrinoDataInputStream input = new TrinoDataInputStream(inputStream, bufferSize);
        assertThat(input.getRetainedSize()).isEqualTo(instanceSize(TrinoDataInputStream.class) + sizeOfByteArray(bufferSize));
    }

    private static void testDataInput(DataInputTester tester)
            throws IOException
    {
        int size = (BUFFER_SIZE * 3) + 10;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(size);
        try (DataOutputStream output = new DataOutputStream(byteArrayOutputStream)) {
            for (int i = 0; i < size / tester.valueSize(); i++) {
                tester.loadValue(output, i);
            }
        }
        byte[] bytes = byteArrayOutputStream.toByteArray();

        testReadForward(tester, bytes);
        testReadReverse(tester, bytes);
        testReadOffEnd(tester, bytes);
    }

    private static void testReadForward(DataInputTester tester, byte[] bytes)
            throws IOException
    {
        TrinoDataInputStream input = createTrinoDataInputStream(bytes);
        for (int i = 0; i < bytes.length / tester.valueSize(); i++) {
            int position = i * tester.valueSize();
            assertThat(input.getPos()).isEqualTo(position);
            tester.verifyValue(input, i);
        }
    }

    private static void testReadReverse(DataInputTester tester, byte[] bytes)
            throws IOException
    {
        TrinoDataInputStream input = createTrinoDataInputStream(bytes);
        for (int i = bytes.length / tester.valueSize() - 1; i >= 0; i--) {
            int position = i * tester.valueSize();
            input.seek(position);
            assertThat(input.getPos()).isEqualTo(position);
            tester.verifyValue(input, i);
        }
    }

    private static void testReadOffEnd(DataInputTester tester, byte[] bytes)
            throws IOException
    {
        TrinoDataInputStream input = createTrinoDataInputStream(bytes);
        ByteStreams.skipFully(input, bytes.length - tester.valueSize() + 1);
        tester.verifyReadOffEnd(input);
    }

    private static String getExpectedStringValue(int index, int size)
            throws IOException
    {
        return ByteSource.concat(cycle(ByteSource.wrap(String.valueOf(index).getBytes(UTF_8)))).slice(0, size).asCharSource(UTF_8).read();
    }

    protected abstract static class DataInputTester
    {
        private final int size;

        public DataInputTester(int size)
        {
            this.size = size;
        }

        public final int valueSize()
        {
            return size;
        }

        public abstract void loadValue(DataOutputStream slice, int valueIndex)
                throws IOException;

        public abstract void verifyValue(TrinoDataInputStream input, int valueIndex)
                throws IOException;

        public void verifyReadOffEnd(TrinoDataInputStream input)
                throws IOException
        {
            try {
                verifyValue(input, 1);
                throw new AssertionError("expected EOFException");
            }
            catch (EOFException expected) {
            }
        }
    }

    private abstract static class SkipDataInputTester
            extends DataInputTester
    {
        public SkipDataInputTester(int size)
        {
            super(size);
        }

        @Override
        public void loadValue(DataOutputStream output, int valueIndex)
                throws IOException
        {
            output.write(new byte[valueSize()]);
        }
    }

    private abstract static class StringDataInputTester
            extends DataInputTester
    {
        public StringDataInputTester(int size)
        {
            super(size);
        }

        @Override
        public final void loadValue(DataOutputStream output, int valueIndex)
                throws IOException
        {
            output.write(getExpectedStringValue(valueIndex, valueSize()).getBytes(UTF_8));
        }

        @Override
        public final void verifyValue(TrinoDataInputStream input, int valueIndex)
                throws IOException
        {
            String actual = readActual(input);
            String expected = getExpectedStringValue(valueIndex, valueSize());
            assertThat(actual).isEqualTo(expected);
        }

        protected abstract String readActual(TrinoDataInputStream input)
                throws IOException;
    }

    private static TrinoDataInputStream createTrinoDataInputStream(byte[] bytes)
            throws IOException
    {
        TrinoInputStream inputStream = getMemoryInputFile(bytes).newStream();
        return new TrinoDataInputStream(inputStream, 16 * 1024);
    }

    private static TrinoInputFile getMemoryInputFile(byte[] bytes)
    {
        return new MemoryInputFile(Location.of("memory:///test"), Slices.wrappedBuffer(bytes));
    }
}
