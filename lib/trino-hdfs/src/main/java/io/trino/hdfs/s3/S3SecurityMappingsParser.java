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
package io.trino.hdfs.s3;

import com.fasterxml.jackson.databind.JsonNode;

import static io.trino.plugin.base.util.JsonUtils.jsonTreeToValue;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;

public class S3SecurityMappingsParser
{
    protected final String jsonPointer;

    protected S3SecurityMappingsParser(S3SecurityMappingConfig config)
    {
        this.jsonPointer = requireNonNull(config.getJsonPointer());
    }

    public S3SecurityMappings parseJSONString(String jsonString)
    {
        JsonNode node = parseJson(jsonString, JsonNode.class);
        JsonNode mappingsNode = node.at(this.jsonPointer);
        return jsonTreeToValue(mappingsNode, S3SecurityMappings.class);
    }
}
