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
package io.trino.plugin.jdbc.mapping;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.nio.file.Path;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.write;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class RuleBasedIdentifierMappingUtils
{
    public static final Duration REFRESH_PERIOD_DURATION = Duration.succinctDuration(1, MILLISECONDS);

    private RuleBasedIdentifierMappingUtils() {}

    public static Path createRuleBasedIdentifierMappingFile()
            throws Exception
    {
        return createRuleBasedIdentifierMappingFile(ImmutableList.of(), ImmutableList.of());
    }

    public static Path createRuleBasedIdentifierMappingFile(List<SchemaMappingRule> schemas, List<TableMappingRule> tables)
            throws Exception
    {
        Path file = createTempFile("identifier-mapping-", ".json");
        file.toFile().deleteOnExit();
        updateRuleBasedIdentifierMappingFile(file, schemas, tables);
        return file;
    }

    public static Path updateRuleBasedIdentifierMappingFile(Path file, List<SchemaMappingRule> schemas, List<TableMappingRule> tables)
            throws Exception
    {
        IdentifierMappingRules mapping = new IdentifierMappingRules(schemas, tables);

        String json = jsonCodec(IdentifierMappingRules.class).toJson(mapping);

        write(file, json.getBytes(UTF_8));
        // Wait to ensure rules have been reloaded
        Thread.sleep(2 * REFRESH_PERIOD_DURATION.toMillis());
        return file;
    }
}
