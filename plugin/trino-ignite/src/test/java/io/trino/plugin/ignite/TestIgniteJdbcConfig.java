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
package io.trino.plugin.ignite;

import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static org.apache.ignite.IgniteJdbcDriver.URL_PREFIX;

public class TestIgniteJdbcConfig
{
    @Test
    public void testIsUrlValid()
    {
        assertValidates(new IgniteJdbcConfig()
                .setConnectionUrl("jdbc:ignite:thin://example.com:10800"));
        assertValidates(new IgniteJdbcConfig()
                .setConnectionUrl("jdbc:ignite:thin://example.com:10800/someschema"));
        assertValidates(new IgniteJdbcConfig()
                .setConnectionUrl("jdbc:ignite:thin://example.com:10800?user=exampleuser"));

        assertFailsConnectionUrlValidation("jdbc:ignite://example.com:10800");
        assertFailsConnectionUrlValidation("jdbc:thin://example.com:10800");
        assertFailsConnectionUrlValidation("ignite:thin://example.com:10800");
    }

    private void assertFailsConnectionUrlValidation(String connectionUrl)
    {
        assertFailsValidation(
                new IgniteJdbcConfig()
                        .setConnectionUrl(connectionUrl),
                "urlValid",
                "JDBC URL for Ignite connector should start with " + URL_PREFIX,
                AssertTrue.class);
    }
}
