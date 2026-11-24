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
package io.trino.plugin.duckdb;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static java.util.Locale.ENGLISH;

final class TestDuckDbJdbcConfig
{
    @ParameterizedTest
    @FieldSource("io.trino.plugin.duckdb.DuckDbJdbcConfig#FORBIDDEN_URL_OPTIONS")
    void testSecurityOptionInUrlRejected(String option)
    {
        assertUrlRejected("jdbc:duckdb:;%s=true".formatted(option));
        // The driver ignores case and whitespace around keys
        assertUrlRejected("jdbc:duckdb:;threads=1; %s = true".formatted(option.toUpperCase(ENGLISH)));
        // The driver trims keys with String.trim, which also removes control characters
        assertUrlRejected("jdbc:duckdb:;\u0000%s=true".formatted(option));
        assertUrlRejected("jdbc:duckdb:;\t%s\u0001=true".formatted(option));
        // An entry without a value is still checked
        assertUrlRejected("jdbc:duckdb:;" + option);
    }

    @Test
    void testOtherOptionsInUrlAllowed()
    {
        assertValidates(config("jdbc:duckdb:"));
        assertValidates(config("jdbc:duckdb:/tmp/test.db"));
        assertValidates(config("jdbc:duckdb:;threads=1;;memory_limit=1GB"));
    }

    @Test
    void testMissingUrl()
    {
        // Only @NotNull reports a missing connection-url
        assertFailsValidation(config(null), "connectionUrl", "must not be null", NotNull.class);
    }

    private static void assertUrlRejected(String url)
    {
        assertFailsValidation(config(url), "urlWithoutSecurityOptions", "DuckDB security options cannot be set in connection-url", AssertTrue.class);
    }

    private static DuckDbJdbcConfig config(String url)
    {
        DuckDbJdbcConfig config = new DuckDbJdbcConfig();
        config.setConnectionUrl(url);
        return config;
    }
}
