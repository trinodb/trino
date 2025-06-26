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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.trino.plugin.openlineage.OpenLineageTrinoFacet.TRINO_METADATA;
import static io.trino.plugin.openlineage.OpenLineageTrinoFacet.TRINO_QUERY_STATISTICS;
import static io.trino.spi.resourcegroups.QueryType.ALTER_TABLE_EXECUTE;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.MERGE;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.spi.resourcegroups.QueryType.UPDATE;

final class TestOpenLineageListenerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenLineageListenerConfig.class)
                .setTrinoURI(null)
                .setNamespace(null)
                .setJobNameFormat("$QUERY_ID")
                .setDisabledFacets(ImmutableSet.of())
                .setIncludeQueryTypes(ImmutableSet.of(
                        ALTER_TABLE_EXECUTE,
                        DELETE,
                        INSERT,
                        MERGE,
                        UPDATE,
                        DATA_DEFINITION)));
    }

    @Test
    void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.trino.uri", "http://testtrino")
                .put("openlineage-event-listener.trino.include-query-types", "SELECT,DELETE")
                .put("openlineage-event-listener.disabled-facets", "trino_metadata,trino_query_statistics")
                .put("openlineage-event-listener.namespace", "testnamespace")
                .put("openlineage-event-listener.job.name-format", "$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123")
                .buildOrThrow();

        OpenLineageListenerConfig expected = new OpenLineageListenerConfig()
                .setTrinoURI(new URI("http://testtrino"))
                .setIncludeQueryTypes(ImmutableSet.of(SELECT, DELETE))
                .setDisabledFacets(ImmutableSet.of(TRINO_METADATA, TRINO_QUERY_STATISTICS))
                .setNamespace("testnamespace")
                .setJobNameFormat("$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123");

        assertFullMapping(properties, expected);
    }

    @Test
    void testIsJobNameFormatValid()
    {
        assertValidates(configWithFormat("abc123"));
        assertValidates(configWithFormat("$QUERY_ID"));
        assertValidates(configWithFormat("$USER"));
        assertValidates(configWithFormat("$SOURCE"));
        assertValidates(configWithFormat("$CLIENT_IP"));
        assertValidates(configWithFormat("$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123"));
        assertValidates(configWithFormat("$QUERY_ID $USER $SOURCE $CLIENT_IP abc123"));

        String failedValidation = "jobNameFormatValid";
        String errorMessage = "Correct job name format may consist of only letters, digits, underscores, commas, spaces, equal signs and predefined values";
        assertFailsValidation(
                configWithFormat("$query_id"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
        assertFailsValidation(
                configWithFormat("$UNKNOWN"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
        assertFailsValidation(
                configWithFormat("$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-$UNKNOWN"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
        assertFailsValidation(
                configWithFormat("${QUERY_ID}"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
        assertFailsValidation(
                configWithFormat("$$QUERY_ID"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
        assertFailsValidation(
                configWithFormat("\\$QUERY_ID"),
                failedValidation,
                errorMessage,
                AssertTrue.class);
    }

    private static OpenLineageListenerConfig configWithFormat(String format)
    {
        return new OpenLineageListenerConfig()
            .setTrinoURI(URI.create("http://testtrino"))
            .setJobNameFormat(format);
    }
}
