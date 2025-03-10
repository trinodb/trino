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
package io.trino.plugin.iceberg.containers;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

public class UnityCatalogContainer
        implements AutoCloseable
{
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();

    private final String catalogName;
    private final String schemaName;
    private final PostgreSQLContainer<?> postgreSql;
    private final GenericContainer<?> unityCatalog;
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public UnityCatalogContainer(String catalogName, String schemaName)
            throws IOException
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schema is null");

        Network network = Network.newNetwork();
        closer.register(network);

        //noinspection resource
        postgreSql = new PostgreSQLContainer<>(DockerImageName.parse("postgres"))
                .withNetwork(network)
                .withNetworkAliases("postgres");
        postgreSql.start();
        closer.register(postgreSql);

        String hibernate =
                """
                hibernate.connection.driver_class=org.postgresql.Driver
                hibernate.connection.url=jdbc:postgresql://postgres:5432/test
                hibernate.connection.username=test
                hibernate.connection.password=test
                hibernate.hbm2ddl.auto=update
                """;

        File hibernateProperties = Files.createTempFile("hibernate", ".properties").toFile();
        Files.writeString(hibernateProperties.toPath(), hibernate);

        //noinspection resource
        unityCatalog = new GenericContainer<>(DockerImageName.parse("ghcr.io/trinodb/testing/unity-catalog:" + getDockerImagesVersion()))
                .withExposedPorts(8080)
                .withNetwork(network)
                .withCopyFileToContainer(MountableFile.forHostPath(hibernateProperties.toPath()), "/unity/etc/conf/hibernate.properties");
        unityCatalog.start();
        closer.register(unityCatalog);

        createCatalog();
        createSchema();
    }

    public String uri()
    {
        return "http://%s:%s/api/2.1/unity-catalog".formatted(unityCatalog.getHost(), unityCatalog.getMappedPort(8080));
    }

    private void createCatalog()
    {
        @Language("JSON")
        String body = "{\"name\": \"" + catalogName + "\"}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(uri() + "/catalogs"))
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    private void createSchema()
    {
        @Language("JSON")
        String body = "{\"name\": \"" + schemaName + "\", \"catalog_name\": \"" + catalogName + "\"}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(uri() + "/schemas"))
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        execute(request);
    }

    public void copyTpchTables(Iterable<TpchTable<?>> tpchTables)
            throws Exception
    {
        File metastoreDir = createTempDirectory("iceberg_query_runner").toFile();
        metastoreDir.deleteOnExit();

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                .build();

        for (TpchTable<?> table : tpchTables) {
            String tableName = table.getTableName();
            queryRunner.execute("CREATE TABLE iceberg.tpch." + tableName + " AS SELECT * FROM tpch.tiny." + tableName);
            String metadataFilePath = (String) queryRunner.execute("SELECT file FROM \"" + tableName + "$metadata_log_entries\" ORDER BY file LIMIT 1").getOnlyValue();
            createTable(tableName, metadataFilePath);
        }
        unityCatalog.copyFileToContainer(MountableFile.forHostPath(metastoreDir.getPath()), metastoreDir.getPath());
        queryRunner.close();
    }

    private void createTable(String tableName, String metadataFilePath)
    {
        @Language("JSON")
        String body = "{" +
                "\"catalog_name\": \"" + catalogName + "\"," +
                "\"schema_name\": \"" + schemaName + "\"," +
                "\"name\": \"" + tableName + "\"," +
                "\"table_type\": \"EXTERNAL\"," +
                "\"data_source_format\": \"DELTA\"," +
                "\"storage_location\": \"" + metadataFilePath + "\"" +
                "}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(uri() + "/tables"))
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        execute(request);

        // TODO https://github.com/unitycatalog/unitycatalog/issues/312 Fix uc_tables directly until the issue is fixed
        execute("UPDATE uc_tables " +
                "SET uniform_iceberg_metadata_location = '" + metadataFilePath + "'" +
                "WHERE name = '" + tableName + "'");
    }

    public void execute(@Language("SQL") String sql)
    {
        try (Connection connection = DriverManager.getConnection(postgreSql.getJdbcUrl(), postgreSql.getUsername(), postgreSql.getPassword());
                Statement statement = connection.createStatement()) {
            //noinspection SqlSourceToSinkFlow
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    private static void execute(Request request)
    {
        StringResponseHandler.StringResponse response = HTTP_CLIENT.execute(request, createStringResponseHandler());

        int status = response.getStatusCode();
        if (status != HttpStatus.OK.code()) {
            throw new IllegalStateException(format("Request '%s' returned unexpected status code: '%d'", request, status));
        }
    }
}
