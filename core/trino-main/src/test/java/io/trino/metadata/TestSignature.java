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
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.function.Signature;
import io.trino.spi.function.Signature.Argument;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSignature
{
    private static final JsonCodec<Signature> CODEC = new JsonCodecFactory(
            new JsonMapperProvider()
                    .withJsonDeserializers(ImmutableMap.of(
                            Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                            TypeSignature.class, new TypeSignatureDeserializer()))
                    .get())
            .prettyPrint()
            .jsonCodec(Signature.class);

    @Test
    public void testSerializationRoundTrip()
    {
        Signature expected = Signature.builder()
                .returnType(BIGINT)
                .argumentType(BOOLEAN)
                .argumentType(DOUBLE)
                .argumentType(VARCHAR)
                .build();

        String json = CODEC.toJson(expected);
        Signature actual = CODEC.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getArguments()).allSatisfy(argument -> assertThat(argument.name()).isEmpty());
    }

    @Test
    public void testArgumentNamesRoundTrip()
    {
        Signature expected = Signature.builder()
                .returnType(VARCHAR.getTypeSignature())
                .argumentType(VARCHAR.getTypeSignature(), "string")
                .argumentType(BIGINT.getTypeSignature(), "from")
                .argumentType(BIGINT.getTypeSignature())
                .build();

        assertThat(expected.getArguments())
                .extracting(Argument::name)
                .containsExactly(Optional.of("string"), Optional.of("from"), Optional.empty());

        Signature actual = CODEC.fromJson(CODEC.toJson(expected));
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getArguments())
                .extracting(Argument::name)
                .containsExactly(Optional.of("string"), Optional.of("from"), Optional.empty());
    }

    @Test
    public void testEqualityIncludesArgumentNames()
    {
        Signature withName = Signature.builder()
                .returnType(BIGINT)
                .argumentType(BIGINT.getTypeSignature(), "x")
                .build();
        Signature withoutName = Signature.builder()
                .returnType(BIGINT)
                .argumentType(BIGINT)
                .build();
        assertThat(withName).isNotEqualTo(withoutName);
    }
}
