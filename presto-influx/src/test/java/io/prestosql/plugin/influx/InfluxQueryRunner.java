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
package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.assertions.Assert;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.testcontainers.containers.InfluxDBContainer;

import java.io.Closeable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.containers.InfluxDBContainer.INFLUXDB_PORT;

public class InfluxQueryRunner
        implements Closeable
{
    public static final String DATABASE = InfluxTpchTestSupport.TEST_DATABASE;
    public static final String USERNAME = "testuser";
    public static final String PASSWORD = "testpass";
    public static final String ADMIN_USERNAME = "admin";
    public static final String ADMIN_PASSWORD = "root";
    private static final String CATALOG = "influx_tests";
    public static final String TPCH_SCHEMA = InfluxTpchTestSupport.TPCH_SCHEMA;
    private final Logger logger = Logger.get("InfluxQueryRunner");
    private final InfluxDBContainer dockerContainer;
    private final DistributedQueryRunner queryRunner;
    private boolean initedTpch;

    public InfluxQueryRunner()
            throws Exception
    {
        dockerContainer = new InfluxDBContainer()
                .withDatabase(DATABASE)
                .withAdmin(ADMIN_USERNAME)
                .withAdminPassword(ADMIN_PASSWORD)
                .withUsername(USERNAME)
                .withPassword(PASSWORD);
        dockerContainer.start();
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TPCH_SCHEMA)
                .setIdentity(Identity.forUser("test")
                        .withRole(CATALOG, new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build()).build();
        queryRunner.installPlugin(new InfluxPlugin());
        queryRunner.createCatalog(CATALOG, "influx",
                new ImmutableMap.Builder<String, String>()
                        .put("influx.host", getHost())
                        .put("influx.port", Integer.toString(getPort()))
                        .put("influx.database", DATABASE)
                        .put("influx.username", USERNAME)
                        .put("influx.password", PASSWORD)
                        .put("influx.read-timeout", "60")
                        .build());
    }

    public DistributedQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public Session createSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(schema)
                .build();
    }

    public String getHost()
    {
        return dockerContainer.getContainerIpAddress();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(INFLUXDB_PORT);
    }

    public void executeDDL(String query)
    {
        try {
            String url = "http://" + getHost() + ":" + getPort() + "/query?db=" + DATABASE + "&q=" + URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
            Assert.assertTrue(
                    new OkHttpClient.Builder()
                            .connectTimeout(10, TimeUnit.SECONDS)
                            .writeTimeout(10, TimeUnit.SECONDS)
                            .readTimeout(30, TimeUnit.SECONDS)
                            .build()
                            .newCall(new Request.Builder()
                                    .url(url)
                                    .addHeader("Authorization", Credentials.basic(ADMIN_USERNAME, ADMIN_PASSWORD))
                                    .build())
                            .execute().isSuccessful());
        }
        catch (Throwable t) {
            Assert.fail("could not execute ddl " + query, t);
        }
    }

    public void createRetentionPolicy(String retentionPolicy)
    {
        executeDDL("CREATE RETENTION POLICY " + retentionPolicy + " ON " + DATABASE + " DURATION INF REPLICATION 1");
    }

    public void write(String retentionPolicy, String... points)
    {
        try {
            String url = "http://" + getHost() + ":" + getPort() + "/write?db=" + DATABASE + "&rp=" + retentionPolicy;
            Assert.assertTrue(new OkHttpClient.Builder()
                    .connectTimeout(10, TimeUnit.SECONDS)
                    .writeTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .build()
                    .newCall(new Request.Builder()
                            .url(url)
                            .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                            .post(RequestBody.create(MediaType.parse("text/plain; charset=utf-8"), String.join("\n", points)))
                            .build())
                    .execute()
                    .isSuccessful());
        }
        catch (Throwable t) {
            Assert.fail("could not write points", t);
        }
    }

    public void initTpch()
            throws Exception
    {
        if (!initedTpch) {
            logger.info("loading tpch data...");
            long startTime = System.nanoTime();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            createRetentionPolicy(TPCH_SCHEMA);
            for (TpchTable<?> table : TpchTable.getTables()) {
                QualifiedObjectName source = new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH));
                MaterializedResult desc = queryRunner.execute("DESC " + source).toTestTypes();
                StringBuilder linePattern = new StringBuilder(table.getTableName());
                int columnId = 0;
                Collection<Integer> dateColumns = new ArrayList<>();
                for (MaterializedRow columnDefinition : desc.getMaterializedRows()) {
                    String columnName = columnDefinition.getField(0).toString();
                    String columnType = columnDefinition.getField(1).toString();
                    linePattern.append(columnId == 0 ? ' ' : ',');
                    linePattern.append(columnName).append('=');
                    if (columnType.contains("char")) {
                        linePattern.append("\"%s\"");
                    }
                    else if (columnType.contains("int")) {
                        linePattern.append("%di");
                    }
                    else if (columnType.equals("date")) {
                        linePattern.append("%di");
                        dateColumns.add(columnId);
                    }
                    else if (columnType.equals("double")) {
                        linePattern.append("%f");
                    }
                    else {
                        throw new Exception("Unhandled column: " + source + " " + columnName + " is " + columnType);
                    }
                    columnId++;
                }
                linePattern.append(" %d");  // the timestamp after the fields
                String sql = "SELECT * FROM " + source;
                MaterializedResult data = queryRunner.execute(sql).toTestTypes();
                Collection<String> points = new ArrayList<>();
                for (MaterializedRow row : data.getMaterializedRows()) {
                    List<Object> columns = new ArrayList<>(row.getFields());
                    for (int dateColumn : dateColumns) {
                        columns.set(dateColumn, ((LocalDate) columns.get(dateColumn)).toEpochDay());
                    }
                    columns.add(startTime + points.size());  // give every row a unique and incrementing timestamp
                    String point = String.format(linePattern.toString(), columns.toArray());
                    points.add(point);
                }
                write(TPCH_SCHEMA, points.toArray(new String[] {}));
            }
            logger.info("loaded tpch in %s", nanosSince(startTime).toString(SECONDS));
        }
        initedTpch = true;
    }

    @Override
    public void close()
    {
        queryRunner.close();
        dockerContainer.close();
    }
}
