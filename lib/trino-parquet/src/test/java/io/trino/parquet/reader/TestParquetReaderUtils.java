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
package io.trino.parquet.reader;

import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesUtils;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static io.trino.parquet.ParquetReaderUtils.readUleb128Int;
import static io.trino.parquet.reader.TestData.randomInt;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetReaderUtils
{
    @Test
    public void testReadUleb128Int()
            throws IOException
    {
        Random random = new Random(1);
        for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
            for (int i = 0; i < 10; i++) {
                int value = randomInt(random, bitWidth);
                SimpleSliceInputStream sliceInputStream = getSliceInputStream(BytesUtils::writeUnsignedVarInt, value);
                assertThat(readUleb128Int(sliceInputStream))
                        .isEqualTo(value);
                assertThat(sliceInputStream.asSlice().length())
                        .isEqualTo(0);
            }
        }
    }

    private static <T> SimpleSliceInputStream getSliceInputStream(ValuesWriter<T> writer, T value)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(value, out);
        return new SimpleSliceInputStream(Slices.wrappedBuffer(out.toByteArray()));
    }

    private interface ValuesWriter<T>
    {
        void write(T value, OutputStream out)
                throws IOException;
    }
}
