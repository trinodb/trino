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
package io.trino.sql.planner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SymbolKeySerializer
        extends JsonSerializer<Symbol>
{
    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    @Override
    public void serialize(Symbol value, JsonGenerator generator, SerializerProvider serializers)
            throws IOException
    {
        String name = ENCODER.encodeToString(value.name().getBytes(UTF_8));
        String type = ENCODER.encodeToString(value.type().getTypeId().getId().getBytes(UTF_8));
        generator.writeFieldName(name + ":" + type);
    }
}
