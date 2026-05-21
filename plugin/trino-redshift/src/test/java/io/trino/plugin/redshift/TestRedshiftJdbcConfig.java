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
package io.trino.plugin.redshift;

import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestRedshiftJdbcConfig
{
    @Test
    public void testIsUrlValid()
    {
        assertValidates(new RedshiftJdbcConfig()
                .setConnectionUrl("jdbc:redshift://datatype.com:5439/dev"));

        assertValidates(new RedshiftJdbcConfig()
                .setConnectionUrl("jdbc:redshift://datatype.com:5439/dev?user=admin&password=secret"));
        assertValidates(new RedshiftJdbcConfig()
                .setConnectionUrl("jdbc:redshift://example.com:5439/dev;ssl=true;user=admin"));
        assertValidates(new RedshiftJdbcConfig()
                .setConnectionUrl("jdbc:redshift://example.com:5439/dev;key=datatype.value"));

        assertFailsConnectionUrlValidation("jdbc:redshift://example.com:5439/dev?datatype.geometry=GEOMETRY");
        assertFailsConnectionUrlValidation("jdbc:redshift://example.com:5439/dev;datatype.geometry=GEOMETRY");
    }

    private void assertFailsConnectionUrlValidation(String connectionUrl)
    {
        assertFailsValidation(
                new RedshiftJdbcConfig()
                        .setConnectionUrl(connectionUrl),
                "urlValid",
                "JDBC URL for redshift connector may contain illegal datatype parameters (CVE-2026-8178)",
                AssertTrue.class);
    }
}
