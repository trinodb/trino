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
package io.trino.hive.formats.avro;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactoryAccessor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static io.trino.hive.formats.avro.TestAvroBase.getLegacyCodecFactory;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAirCompressorAsAvroCodec
{
    @ParameterizedTest
    @EnumSource(AvroCompressionKind.class)
    public void testAirCompressorCompatibility(AvroCompressionKind kind)
            throws IOException
    {
        Supplier<byte[]> bytes = randomOriginalBytes();
        Codec trinoCodec = CodecFactoryAccessor.createCodecInstance(kind.getTrinoCodecFactory());
        Codec avroCodec = CodecFactoryAccessor.createCodecInstance(getLegacyCodecFactory(kind));

        ByteBuffer expected = ByteBuffer.wrap(bytes.get());
        ByteBuffer trinoDecompressedFromAvroCompressed = trinoCodec.decompress(avroCodec.compress(ByteBuffer.wrap(bytes.get())));
        ByteBuffer avroDecompressedFromTrinoCompressed = avroCodec.decompress(trinoCodec.compress(ByteBuffer.wrap(bytes.get())));
        ByteBuffer trinoRoundTrip = trinoCodec.decompress(trinoCodec.compress(ByteBuffer.wrap(bytes.get())));
        ByteBuffer avroRoundTrip = avroCodec.decompress(avroCodec.compress(ByteBuffer.wrap(bytes.get())));

        assertThat(trinoDecompressedFromAvroCompressed).isEqualTo(expected);
        assertThat(avroDecompressedFromTrinoCompressed).isEqualTo(expected);
        assertThat(trinoRoundTrip).isEqualTo(expected);
        assertThat(avroRoundTrip).isEqualTo(expected);
    }

    private Supplier<byte[]> randomOriginalBytes()
    {
        byte[] b = generateRandomByteArray();
        return () -> Arrays.copyOf(b, b.length);
    }

    private byte[] generateRandomByteArray()
    {
        int n = ThreadLocalRandom.current().nextInt(1000, 10000);
        byte[] a = new byte[n];
        ThreadLocalRandom.current().nextBytes(a);
        return a;
    }
}
