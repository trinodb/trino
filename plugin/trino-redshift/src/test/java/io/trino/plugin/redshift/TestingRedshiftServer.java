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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

import java.time.Duration;

import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

public final class TestingRedshiftServer
{
    private TestingRedshiftServer() {}

    public static final String JDBC_ENDPOINT = requiredNonEmptySystemProperty("test.redshift.jdbc.endpoint");
    public static final String JDBC_USER = requiredNonEmptySystemProperty("test.redshift.jdbc.user");
    public static final String JDBC_PASSWORD = requiredNonEmptySystemProperty("test.redshift.jdbc.password");

    public static final String TEST_DATABASE = "testdb";
    public static final String TEST_SCHEMA = "test_schema";

    public static final String JDBC_URL = "jdbc:redshift://" + JDBC_ENDPOINT + TEST_DATABASE;

    public static void executeInRedshiftWithRetry(String sql)
    {
        Failsafe.with(RetryPolicy.builder()
                        .handleIf(e -> e.getMessage().matches(".* concurrent transaction .*"))
                        .withDelay(Duration.ofSeconds(10))
                        .withMaxRetries(3)
                        .build())
                .run(() -> executeInRedshift(sql));
    }

    public static void executeInRedshift(String sql, Object... parameters)
    {
        executeInRedshift(handle -> handle.execute(sql, parameters));
    }

    public static <E extends Exception> void executeInRedshift(HandleConsumer<E> consumer)
            throws E
    {
        executeWithRedshift(consumer.asCallback());
    }

    public static <T, E extends Exception> T executeWithRedshift(HandleCallback<T, E> callback)
            throws E
    {
        return Jdbi.create(JDBC_URL, JDBC_USER, JDBC_PASSWORD).withHandle(callback);
    }
}
