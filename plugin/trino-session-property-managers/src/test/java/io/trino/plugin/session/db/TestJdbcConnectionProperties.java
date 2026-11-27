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
package io.trino.plugin.session.db;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.trino.plugin.session.db.JdbcConnectionProperties.forJdbi;
import static io.trino.plugin.session.db.JdbcConnectionProperties.timeouts;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdbcConnectionProperties
{
    @Test
    public void testPostgresqlTimeoutsInSeconds()
    {
        assertThat(timeouts(config("jdbc:postgresql://host/db")))
                .isEqualTo(ImmutableMap.of("connectTimeout", "10", "socketTimeout", "30"));
    }

    @Test
    public void testMysqlTimeoutsInMilliseconds()
    {
        assertThat(timeouts(config("jdbc:mysql://host/db")))
                .isEqualTo(ImmutableMap.of("connectTimeout", "10000", "socketTimeout", "30000"));
    }

    @Test
    public void testFractionalSecondsRoundUpForPostgresql()
    {
        DbSessionPropertyManagerConfig config = config("jdbc:postgresql://host/db").setSocketTimeout(new Duration(1500, MILLISECONDS));
        assertThat(timeouts(config)).containsEntry("socketTimeout", "2");
    }

    @Test
    public void testTimeoutSetInUrlIsNotOverridden()
    {
        assertThat(timeouts(config("jdbc:postgresql://host/db?socketTimeout=120")))
                .isEqualTo(ImmutableMap.of("connectTimeout", "10"));
        assertThat(timeouts(config("jdbc:mysql://host/db?useSSL=false&connectTimeout=5000")))
                .isEqualTo(ImmutableMap.of("socketTimeout", "30000"));
    }

    @Test
    public void testCredentialsOnlyWhenConfigured()
    {
        Properties noCredentials = forJdbi(config("jdbc:postgresql://host/db"));
        assertThat(noCredentials).doesNotContainKeys("user", "password");

        Properties userOnly = forJdbi(config("jdbc:postgresql://host/db").setConfigDbUser("alice"));
        assertThat(userOnly).containsEntry("user", "alice").doesNotContainKey("password");

        Properties both = forJdbi(config("jdbc:postgresql://host/db").setConfigDbUser("alice").setConfigDbPassword("secret"));
        assertThat(both).containsEntry("user", "alice").containsEntry("password", "secret");
    }

    @Test
    public void testUnsupportedUrl()
    {
        assertThatThrownBy(() -> timeouts(config("jdbc:sqlite:/tmp/session.db")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only PostgreSQL and MySQL are supported");
    }

    private static DbSessionPropertyManagerConfig config(String url)
    {
        return new DbSessionPropertyManagerConfig().setConfigDbUrl(url);
    }
}
