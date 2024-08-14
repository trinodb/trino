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
package io.trino.plugin.hsqldb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHsqlDbJdbcConfig
{
    @Test
    public void testIsUrlValid()
    {
        assertThat(isUrlValid("jdbc:hsqldb:mem")).isTrue();
        assertThat(isUrlValid("jdbc:hsqldb:hsql://localhost/")).isTrue();
        assertThat(isUrlValid("jdbc:nothsqldb:mem")).isFalse();
        assertThat(isUrlValid("jdbc:nothsqldb:hsql://localhost/")).isFalse();
    }

    private static boolean isUrlValid(String url)
    {
        HsqlDbJdbcConfig config = new HsqlDbJdbcConfig();
        config.setConnectionUrl(url);
        return config.isUrlValid();
    }
}
