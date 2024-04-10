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
package io.trino.plugin.doris;

import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestDorisJdbcConfig
{
    @Test
    public void testIsUrlValid()
    {
        assertValidates(new DorisJdbcConfig()
                .setConnectionUrl("jdbc:mysql://example.net:9030"));
        assertValidates(new DorisJdbcConfig()
                .setConnectionUrl("jdbc:mysql://example.net:9030/"));
        assertValidates(new DorisJdbcConfig()
                .setConnectionUrl("jdbc:mysql://example.net:9030?user=exampleuser"));

        assertFailsValidation(
                new DorisJdbcConfig().setConnectionUrl("jdbc:starrocks://"),
                "urlValid",
                "Invalid JDBC URL for StarRocks connector",
                AssertTrue.class);
        assertFailsValidation(
                new DorisJdbcConfig().setConnectionUrl("jdbc:mysq://localhost:9030"),
                "urlValid",
                "Invalid JDBC URL for StarRocks connector",
                AssertTrue.class);
    }

    @Test
    public void testIsUrlWithoutDatabase()
    {
        assertFailsValidation(
                new DorisJdbcConfig().setConnectionUrl("jdbc:mysql://example.net/somedatabase"),
                "urlWithoutDatabase",
                "Database (catalog) must not be specified in JDBC URL for StarRocks connector",
                AssertTrue.class);

        assertFailsValidation(
                new DorisJdbcConfig().setConnectionUrl("jdbc:mysql://example.net:9030/somedatabase"),
                "urlWithoutDatabase",
                "Database (catalog) must not be specified in JDBC URL for StarRocks connector",
                AssertTrue.class);
    }
}
