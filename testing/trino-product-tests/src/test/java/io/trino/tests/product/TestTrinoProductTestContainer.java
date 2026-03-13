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
package io.trino.tests.product;

import io.trino.testing.containers.TrinoProductTestContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link TrinoProductTestContainer}.
 * This test actually starts a Trino container and runs queries against it.
 */
@Testcontainers
@Tag("smoke")
class TestTrinoProductTestContainer
{
    @Container
    static TrinoContainer trino = TrinoProductTestContainer.builder()
            .build();

    @Test
    void testSimpleQuery()
            throws Exception
    {
        try (Connection conn = TrinoProductTestContainer.createConnection(trino);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void testJdbcUrlAvailable()
    {
        String jdbcUrl = trino.getJdbcUrl();
        assertThat(jdbcUrl).startsWith("jdbc:trino://");
    }

    @Test
    void testBuilderCreatesConfiguredContainer()
    {
        // Verify builder pattern works correctly
        TrinoContainer container = TrinoProductTestContainer.builder()
                .withVersion("latest")
                .build();
        assertThat(container).isNotNull();
        // Don't start the container, just verify it's configured
    }
}
