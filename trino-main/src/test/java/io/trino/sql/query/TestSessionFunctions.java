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
package io.trino.sql.query;

import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.sql.SqlPath;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSessionFunctions
{
    @Test
    public void testCurrentUser()
    {
        Session session = testSessionBuilder()
                .setIdentity(Identity.ofUser("test_current_user"))
                .build();
        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            assertThat(queryAssertions.query("SELECT CURRENT_USER")).matches("SELECT CAST('" + session.getUser() + "' AS VARCHAR)");
        }
    }

    @Test
    public void testCurrentPath()
    {
        Session session = testSessionBuilder()
                .setPath(new SqlPath(Optional.of("testPath")))
                .build();

        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            assertThat(queryAssertions.query("SELECT CURRENT_PATH")).matches("SELECT CAST('" + session.getPath().toString() + "' AS VARCHAR)");
        }

        Session emptyPathSession = testSessionBuilder()
                .setPath(new SqlPath(Optional.empty()))
                .build();

        try (QueryAssertions queryAssertions = new QueryAssertions(emptyPathSession)) {
            assertThat(queryAssertions.query("SELECT CURRENT_PATH")).matches("VALUES VARCHAR ''");
        }
    }
}
