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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoJsonCodec
{
    @Test
    public void testTrailingContent()
            throws Exception
    {
        TrinoJsonCodec<ClientTypeSignature> codec = jsonCodec(ClientTypeSignature.class);

        String json = "{\"rawType\":\"bigint\",\"arguments\":[]}";
        assertThat(codec.fromJson(json).getRawType()).isEqualTo("bigint");

        String jsonWithTrailingContent = json + " trailer";
        assertThatThrownBy(() -> codec.fromJson(jsonWithTrailingContent))
                .isInstanceOf(JsonParseException.class)
                .hasMessageStartingWith("Unrecognized token 'trailer': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");

        assertThatThrownBy(() -> codec.fromJson(new ByteArrayInputStream(jsonWithTrailingContent.getBytes(UTF_8))))
                .isInstanceOf(JsonParseException.class)
                .hasMessageStartingWith("Unrecognized token 'trailer': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");

        String jsonWithTrailingJsonContent = json + " \"valid json value\"";
        assertThatThrownBy(() -> codec.fromJson(jsonWithTrailingJsonContent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Found characters after the expected end of input");

        assertThatThrownBy(() -> codec.fromJson(new ByteArrayInputStream(jsonWithTrailingJsonContent.getBytes(UTF_8))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Found characters after the expected end of input");
    }

    @Test
    public void testUndecodableUtf8()
            throws IOException
    {
        byte[] data = new byte[] {'{', '"', 't', 'e', 's', 't', '"', ':', '"', 0xa, 0x43, 0x41, 0x44, 0x5f, 0x43, 0x41, 0x44, 0x08, '"', '}'};

        TrinoJsonCodec<TestClass> codec = jsonCodec(TestClass.class);
        TestClass value = codec.fromJson(new ByteArrayInputStream(data));

        assertThat(value.test).isEqualTo(new String(new byte[] {0xa, 0x43, 0x41, 0x44, 0x5f, 0x43, 0x41, 0x44, 0x08}, UTF_8));
    }

    public static class TestClass
    {
        private String test;

        @JsonCreator
        public TestClass(@JsonProperty("test") String test)
        {
            this.test = test;
        }
    }
}
