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
package io.trino.plugin.resourcegroups.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSupportedDatabase
{
    @ParameterizedTest
    @CsvSource({
            "jdbc:mysql://host/db, /db/migration/mysql",
            "jdbc:postgresql://host/db, /db/migration/postgresql",
            "jdbc:oracle:thin:@host:1521/db, /db/migration/oracle",
    })
    public void testKnownUrlsMapToCorrectLocation(String url, String expectedLocation)
    {
        assertThat(SupportedDatabase.requireSupported(url).getMigrationLocation())
                .isEqualTo(expectedLocation);
    }

    @Test
    public void testUnsupportedUrlThrows()
    {
        assertThatThrownBy(() -> SupportedDatabase.requireSupported("jdbc:h2:mem:test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbc:h2:mem:test")
                .hasMessageContaining("Only PostgreSQL, MySQL, and Oracle are supported");
    }
}
