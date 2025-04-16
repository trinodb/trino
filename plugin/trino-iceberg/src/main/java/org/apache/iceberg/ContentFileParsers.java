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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;

public final class ContentFileParsers
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private ContentFileParsers() {}

    public static String toJson(ContentFile<?> contentFile, PartitionSpec spec)
    {
        return ContentFileParser.toJson(contentFile, spec);
    }

    public static ContentFile<?> fromJson(String jsonNode, PartitionSpec spec)
    {
        try {
            return ContentFileParser.fromJson(OBJECT_MAPPER.readTree(jsonNode), spec);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
