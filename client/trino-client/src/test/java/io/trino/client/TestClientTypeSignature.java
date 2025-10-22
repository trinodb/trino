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
package io.trino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.StandardTypes;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClientTypeSignature
{
    public static final TrinoJsonCodec<ClientTypeSignature> CLIENT_TYPE_SIGNATURE_CODEC;

    static {
        CLIENT_TYPE_SIGNATURE_CODEC = jsonCodec(ClientTypeSignature.class);
    }

    @Test
    public void testJsonRoundTrip()
    {
        ClientTypeSignature bigint = new ClientTypeSignature(StandardTypes.BIGINT);
        assertJsonRoundTrip(bigint);
        assertJsonRoundTrip(new ClientTypeSignature(
                "array",
                ImmutableList.of(ClientTypeSignatureParameter.ofType(bigint))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "foo",
                ImmutableList.of(ClientTypeSignatureParameter.ofLong(42))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "row",
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo")), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar")), bigint)))));
    }

    @Test
    public void testStringSerialization()
    {
        ClientTypeSignature bigint = new ClientTypeSignature(StandardTypes.BIGINT);
        ClientTypeSignature varchar = new ClientTypeSignature(StandardTypes.VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(50)));
        assertThat(bigint.toString()).isEqualTo("bigint");
        assertThat(varchar.toString()).isEqualTo("varchar(50)");
        ClientTypeSignature array = new ClientTypeSignature(StandardTypes.ARRAY, ImmutableList.of(ClientTypeSignatureParameter.ofType(new ClientTypeSignature(StandardTypes.BIGINT))));
        assertThat(array.toString()).isEqualTo("array(bigint)");
        ClientTypeSignature row = new ClientTypeSignature(
                StandardTypes.ROW,
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo")), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar")), bigint))));
        assertThat(row.toString()).isEqualTo("row(foo bigint,bar bigint)");
    }

    private static void assertJsonRoundTrip(ClientTypeSignature signature)
    {
        try {
            String json = CLIENT_TYPE_SIGNATURE_CODEC.toJson(signature);
            ClientTypeSignature copy = CLIENT_TYPE_SIGNATURE_CODEC.fromJson(json);
            assertThat(copy).isEqualTo(signature);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
