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
package io.trino.tests.product.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.query.JdbcConnectionsPool;
import io.trino.tempto.query.JdbcConnectivityParamsState;
import io.trino.tempto.query.JdbcQueryExecutor;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.resolver.ArtifactResolver.MAVEN_CENTRAL_URI;
import static io.airlift.resolver.ArtifactResolver.USER_LOCAL_REPO;
import static java.util.Map.entry;

public final class DeltaQueryExecutors
{
    private static final Logger log = Logger.get(DeltaQueryExecutors.class);

    private static final Map<String, Map.Entry<String, String>> ARTIFACTS = ImmutableMap.<String, Map.Entry<String, String>>builder()
            .put("org.apache.hive.jdbc.HiveDriver", entry("org.apache.hive:hive-jdbc:jar:standalone:3.1.3", "hive-jdbc-3.1.3-standalone.jar"))
            .put("com.databricks.client.jdbc.Driver", entry("com.databricks:databricks-jdbc:2.6.32", "databricks-jdbc-2.6.32.jar"))
            .buildOrThrow();

    @GuardedBy("DRIVERS")
    private static final Map<String, String> DRIVERS = new HashMap<>();
    @GuardedBy("JDBC_EXECUTORS")
    private static final Map<JdbcConnectivityParamsState, JdbcQueryExecutor> JDBC_EXECUTORS = new HashMap<>();

    private static final RetryPolicy<String> loadDatabaseDriverRetryPolicy = RetryPolicy.<String>builder()
            .withMaxRetries(30)
            .withDelay(Duration.ofSeconds(10))
            .onRetry(event -> log.warn(event.getLastException(), "Download failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private DeltaQueryExecutors() {}

    public static JdbcQueryExecutor createDeltaQueryExecutor(TestContext testContext)
    {
        JdbcConnectivityParamsState jdbcParamsState = testContext.getDependency(JdbcConnectivityParamsState.class, "delta");
        JdbcConnectionsPool jdbcConnectionsPool = testContext.getDependency(JdbcConnectionsPool.class);
        synchronized (JDBC_EXECUTORS) {
            return JDBC_EXECUTORS.computeIfAbsent(jdbcParamsState, param -> new JdbcQueryExecutor(withRuntimeJar(param), jdbcConnectionsPool, testContext));
        }
    }

    private static JdbcConnectivityParamsState withRuntimeJar(JdbcConnectivityParamsState jdbcParamsState)
    {
        String jarFilePath;
        synchronized (DRIVERS) {
            jarFilePath = DRIVERS.computeIfAbsent(jdbcParamsState.driverClass, className -> Failsafe.with(loadDatabaseDriverRetryPolicy)
                    .get(() -> loadDatabaseDriverJar(className)));
        }

        return JdbcConnectivityParamsState.builder()
                .setName(jdbcParamsState.getName().orElseThrow())
                .setDriverClass(jdbcParamsState.driverClass)
                .setUrl(jdbcParamsState.url)
                .setUser(jdbcParamsState.user)
                .setPassword(jdbcParamsState.password)
                .setPooling(jdbcParamsState.pooling)
                .setJar(Optional.of(jarFilePath))
                .setPrepareStatements(jdbcParamsState.prepareStatements)
                .setKerberosPrincipal(jdbcParamsState.kerberosPrincipal)
                .setKerberosKeytab(jdbcParamsState.kerberosKeytab)
                .build();
    }

    private static String loadDatabaseDriverJar(String driverClassName)
    {
        // TODO Add support for maven coordinate in tempto
        String coords = ARTIFACTS.get(driverClassName).getKey();
        String jar = ARTIFACTS.get(driverClassName).getValue();

        ArtifactResolver resolver = new ArtifactResolver(USER_LOCAL_REPO, ImmutableList.of(MAVEN_CENTRAL_URI));
        return resolver.resolveArtifacts(new DefaultArtifact(coords)).stream()
                .filter(artifact -> artifact.getFile().getName().equals(jar))
                .map(artifact -> artifact.getFile().getAbsolutePath())
                .collect(onlyElement());
    }
}
