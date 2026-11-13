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
package io.trino.filesystem.s3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;

import static io.trino.plugin.base.util.JsonUtils.jsonTreeToValue;
import static io.trino.plugin.base.util.JsonUtils.parseJson;

final class S3SecurityMappingsParser
{
    private S3SecurityMappingsParser() {}

    public static S3SecurityMappings parseSecurityMappings(String json, String jsonPointer, SecretsResolver secretsResolver)
    {
        return parseSecurityMappings(parseJson(json, JsonNode.class), jsonPointer, secretsResolver);
    }

    public static S3SecurityMappings parseSecurityMappings(byte[] json, String jsonPointer, SecretsResolver secretsResolver)
    {
        // Parsing bytes lets Jackson detect the encoding, including byte order marks
        return parseSecurityMappings(parseJson(json, JsonNode.class), jsonPointer, secretsResolver);
    }

    private static S3SecurityMappings parseSecurityMappings(JsonNode root, String jsonPointer, SecretsResolver secretsResolver)
    {
        // Secrets are resolved within parsed string values rather than in the raw JSON text,
        // so that secret values cannot break or alter the JSON structure
        JsonNode mappingsNode = root.at(jsonPointer);
        return jsonTreeToValue(resolveSecrets(mappingsNode, jsonPointer, secretsResolver), S3SecurityMappings.class);
    }

    private static JsonNode resolveSecrets(JsonNode node, String path, SecretsResolver secretsResolver)
    {
        if (node.isTextual()) {
            String resolved = secretsResolver.getResolvedConfiguration(ImmutableMap.of(path, node.textValue()), (_, throwable) -> {
                throw new IllegalArgumentException("Failed to resolve secret in security mapping at '%s': %s".formatted(path, throwable.getMessage()), throwable);
            }).get(path);
            return TextNode.valueOf(resolved);
        }
        if (node instanceof ObjectNode objectNode) {
            ObjectNode resolved = objectNode.objectNode();
            objectNode.properties().forEach(entry -> resolved.set(entry.getKey(), resolveSecrets(entry.getValue(), path + "/" + entry.getKey(), secretsResolver)));
            return resolved;
        }
        if (node instanceof ArrayNode arrayNode) {
            ArrayNode resolved = arrayNode.arrayNode();
            for (int i = 0; i < arrayNode.size(); i++) {
                resolved.add(resolveSecrets(arrayNode.get(i), path + "/" + i, secretsResolver));
            }
            return resolved;
        }
        return node;
    }
}
