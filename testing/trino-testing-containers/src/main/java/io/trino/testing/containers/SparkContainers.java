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
package io.trino.testing.containers;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

public final class SparkContainers
{
    private SparkContainers() {}

    public static WaitStrategy sparkThriftServerWaitStrategy(int port)
    {
        return sparkThriftServerWaitStrategy(port, "");
    }

    public static WaitStrategy sparkThriftServerWaitStrategy(int port, String database)
    {
        String jdbcUrl = "jdbc:hive2://localhost:" + port;
        if (!database.isEmpty()) {
            jdbcUrl += "/" + database;
        }
        return Wait.forSuccessfulCommand("SPARK_BEELINE_MEMORY=256m timeout 30s /spark/bin/beeline -u %s -n hive -e 'SELECT 1'".formatted(jdbcUrl))
                .withStartupTimeout(Duration.ofMinutes(3));
    }
}
