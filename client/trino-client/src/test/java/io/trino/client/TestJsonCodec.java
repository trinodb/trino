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

import com.fasterxml.jackson.core.JsonParseException;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;

import static io.trino.client.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestJsonCodec
{
    @Test
    public void testTrailingContent()
            throws Exception
    {
        JsonCodec<ClientTypeSignature> codec = jsonCodec(ClientTypeSignature.class);

        String json = "{\"rawType\":\"bigint\",\"arguments\":[]}";
        assertEquals(codec.fromJson(json).getRawType(), "bigint");

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
}
