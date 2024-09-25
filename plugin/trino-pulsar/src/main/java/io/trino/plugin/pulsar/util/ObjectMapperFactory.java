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
package io.trino.plugin.pulsar.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal;

public class ObjectMapperFactory
{
    private ObjectMapperFactory()
    { }

    private static ObjectMapper create()
    {
        ObjectMapper mapper = new ObjectMapper();
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        return mapper;
    }

    public static com.fasterxml.jackson.databind.JsonNode toOriginalJsonNode(JsonNode jsonNode) throws JsonProcessingException
    {
        return ObjectMapperFactory.getThreadLocal().readTree(jsonNode.toString());
    }

    private static final FastThreadLocal<ObjectMapper> JSON_MAPPER = new FastThreadLocal<ObjectMapper>()
    {
        @Override
        protected ObjectMapper initialValue() throws Exception
        {
            return create();
        }
    };

    public static ObjectMapper getThreadLocal()
    {
        return JSON_MAPPER.get();
    }
}
