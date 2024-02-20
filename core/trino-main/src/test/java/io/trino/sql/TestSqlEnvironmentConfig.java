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
package io.trino.sql;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestSqlEnvironmentConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SqlEnvironmentConfig.class)
                .setPath("")
                .setDefaultCatalog(null)
                .setDefaultSchema(null)
                .setDefaultFunctionCatalog(null)
                .setDefaultFunctionSchema(null)
                .setForcedSessionTimeZone(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sql.path", "a.b, c.d, memory.functions")
                .put("sql.default-catalog", "some-catalog")
                .put("sql.default-schema", "some-schema")
                .put("sql.default-function-catalog", "memory")
                .put("sql.default-function-schema", "functions")
                .put("sql.forced-session-time-zone", "UTC")
                .buildOrThrow();

        SqlEnvironmentConfig expected = new SqlEnvironmentConfig()
                .setPath("a.b, c.d, memory.functions")
                .setDefaultCatalog("some-catalog")
                .setDefaultSchema("some-schema")
                .setDefaultFunctionCatalog("memory")
                .setDefaultFunctionSchema("functions")
                .setForcedSessionTimeZone("UTC");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidPath()
    {
        assertFailsValidation(
                new SqlEnvironmentConfig()
                        .setPath("too.many.parts"),
                "sqlPathValid",
                "sql.path must be a valid SQL path",
                AssertTrue.class);
    }

    @Test
    public void testFunctionCatalogSetWithoutSchema()
    {
        assertFailsValidation(
                new SqlEnvironmentConfig()
                        .setDefaultFunctionCatalog("memory"),
                "bothFunctionCatalogAndSchemaSet",
                "sql.default-function-catalog and sql.default-function-schema must be set together",
                AssertTrue.class);
    }

    @Test
    public void testFunctionSchemaSetWithoutCatalog()
    {
        assertFailsValidation(
                new SqlEnvironmentConfig()
                        .setDefaultFunctionSchema("functions"),
                "bothFunctionCatalogAndSchemaSet",
                "sql.default-function-catalog and sql.default-function-schema must be set together",
                AssertTrue.class);
    }

    @Test
    public void testFunctionSchemaNotInSqlPath()
    {
        assertFailsValidation(
                new SqlEnvironmentConfig()
                        .setDefaultFunctionCatalog("memory")
                        .setDefaultFunctionSchema("functions"),
                "functionSchemaInSqlPath",
                "default function schema must be in the default SQL path",
                AssertTrue.class);
    }
}
