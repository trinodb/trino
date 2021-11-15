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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.sql.SqlPath;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
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

    @Test
    public void testCurrentCatalog()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            Session session = testSessionBuilder()
                    .setCatalog("trino_rocks")
                    .build();

            assertThat(assertions.query(session, "SELECT CURRENT_CATALOG"))
                    .matches("VALUES CAST('" + session.getCatalog().get() + "' AS VARCHAR)");

            session = testSessionBuilder()
                    .setCatalog(Optional.empty())
                    .setSchema(Optional.empty())
                    .build();
            assertThat(assertions.query(session, "SELECT CURRENT_CATALOG"))
                    .matches("VALUES CAST(NULL AS VARCHAR)");
        }
    }

    @Test
    public void testCurrentSchema()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            Session session = testSessionBuilder()
                    .setSchema("trino_rocks")
                    .build();

            assertThat(assertions.query(session, "SELECT CURRENT_SCHEMA"))
                    .matches("VALUES CAST('" + session.getSchema().get() + "' AS VARCHAR)");

            session = testSessionBuilder()
                    .setSchema(Optional.empty())
                    .build();
            assertThat(assertions.query(session, "SELECT CURRENT_SCHEMA"))
                    .matches("VALUES CAST(NULL AS VARCHAR)");
        }
    }

    @Test
    public void testCurrentGroups()
    {
        Identity identityWithoutGroups = Identity.ofUser("test_current_user");
        Session session;

        session = testSessionBuilder()
                .setIdentity(identityWithoutGroups)
                .build();
        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            assertThat(queryAssertions.query("SELECT current_groups()")).matches("SELECT CAST(ARRAY[] AS ARRAY(VARCHAR))");
        }

        Set<String> groups = ImmutableSet.of("group_a", "group_b");
        Identity identityWithGroups = new Identity.Builder("test_current_user").withGroups(groups).build();
        session = testSessionBuilder()
                .setIdentity(identityWithGroups)
                .build();
        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            assertThat(queryAssertions.query("SELECT array_sort(current_groups())"))
                    .matches(format("SELECT CAST(ARRAY[%s] AS ARRAY(VARCHAR))", groups.stream().map(e -> format("'%s'", e)).collect(joining(","))));
        }
    }
}
