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
package io.prestosql.client;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestClientTypeSignature
{
    public static final JsonCodec<ClientTypeSignature> CLIENT_TYPE_SIGNATURE_CODEC;

    static {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        CLIENT_TYPE_SIGNATURE_CODEC = codecFactory.jsonCodec(ClientTypeSignature.class);
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
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo", false)), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar", false)), bigint)))));
    }

    @Test
    public void testStringSerialization()
    {
        ClientTypeSignature bigint = new ClientTypeSignature(StandardTypes.BIGINT);
        assertEquals(bigint.toString(), "bigint");
        ClientTypeSignature varchar = new ClientTypeSignature(StandardTypes.VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(50)));
        assertEquals(varchar.toString(), "varchar(50)");
        ClientTypeSignature array = new ClientTypeSignature(StandardTypes.ARRAY, ImmutableList.of(ClientTypeSignatureParameter.ofType(new ClientTypeSignature(StandardTypes.BIGINT))));
        assertEquals(array.toString(), "array(bigint)");
        ClientTypeSignature row = new ClientTypeSignature(
                StandardTypes.ROW,
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo", false)), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar", false)), bigint))));
        assertEquals(row.toString(), "row(foo bigint,bar bigint)");
    }

    private static void assertJsonRoundTrip(ClientTypeSignature signature)
    {
        String json = CLIENT_TYPE_SIGNATURE_CODEC.toJson(signature);
        ClientTypeSignature copy = CLIENT_TYPE_SIGNATURE_CODEC.fromJson(json);
        assertEquals(copy, signature);
    }
}
