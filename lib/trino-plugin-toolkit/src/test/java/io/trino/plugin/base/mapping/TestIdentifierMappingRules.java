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
package io.trino.plugin.base.mapping;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIdentifierMappingRules
{
    @Test
    public void testParsing()
    {
        // The JSON format is part of the user interface, changing this may affect users.

        String json = "{\n" +
                "  \"schemas\" : [ {\n" +
                "    \"remoteSchema\" : \"remote_schema\",\n" +
                "    \"mapping\" : \"trino_schema\"\n" +
                "  } ],\n" +
                "  \"tables\" : [ {\n" +
                "    \"remoteSchema\" : \"remote_schema\",\n" +
                "    \"remoteTable\" : \"remote_table\",\n" +
                "    \"mapping\" : \"trino_table\"\n" +
                "  } ]\n" +
                "}";

        JsonCodec<IdentifierMappingRules> identifierMappingRulesJsonCodec = JsonCodec.jsonCodec(IdentifierMappingRules.class);
        assertThat(identifierMappingRulesJsonCodec.fromJson(json))
                .isEqualTo(new IdentifierMappingRules(
                        ImmutableList.of(new SchemaMappingRule("remote_schema", "trino_schema")),
                        ImmutableList.of(new TableMappingRule("remote_schema", "remote_table", "trino_table"))))
                .isNotEqualTo(new IdentifierMappingRules(ImmutableList.of(), ImmutableList.of()));
    }
}
