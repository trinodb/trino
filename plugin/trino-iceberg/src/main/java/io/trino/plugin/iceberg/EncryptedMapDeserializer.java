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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

public class EncryptedMapDeserializer
        extends JsonDeserializer<Map<String, String>>
{
    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext ctx)
            throws IOException
    {
        String decrypted = CredentialsEncryptorHolder.get().decrypt(p.getText());
        return p.getCodec().readValue(p.getCodec().getFactory().createParser(decrypted), new TypeReference<>() {});
    }
}
