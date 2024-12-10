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
package io.trino.jdbc;

import io.airlift.log.Logging;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spooling.filesystem.FileSystemSpoolingPlugin;
import io.trino.testing.containers.Minio;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.lang.String.format;
import static java.util.Base64.getEncoder;

/**
 * An integration test for JDBC client interacting with Trino server.
 */
public abstract class TestJdbcResultSetWithEncoding
        extends BaseTestJdbcResultSet
{
    private static final String BUCKET_NAME = "segments" + UUID.randomUUID();
    private Minio minio;
    private TestingTrinoServer server;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = createTestingServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");

        try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA memory.tiny");
            statement.executeUpdate("CREATE TABLE memory.tiny.lineitem as SELECT * from tpch.tiny.lineitem");
        }
    }

    protected TestingTrinoServer createTestingServer()
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(BUCKET_NAME, true);

        Map<String, String> filesystemProperties = new java.util.HashMap<>();
        filesystemProperties.put("fs.s3.enabled", "true");
        filesystemProperties.put("fs.location", "s3://" + BUCKET_NAME + "/segments");
        filesystemProperties.put("fs.segment.encryption", "false");
        filesystemProperties.put("s3.endpoint", minio.getMinioAddress());
        filesystemProperties.put("s3.region", MINIO_REGION);
        filesystemProperties.put("s3.aws-access-key", MINIO_ACCESS_KEY);
        filesystemProperties.put("s3.aws-secret-key", MINIO_SECRET_KEY);
        filesystemProperties.put("s3.path-style-access", "true");
        TestingTrinoServer server = TestingTrinoServer.builder()
                .addProperty("experimental.protocol.spooling.enabled", "true")
                .addProperty("protocol.spooling.shared-secret-key", randomAES256Key())
                .build();
        server.installPlugin(new FileSystemSpoolingPlugin());
        server.loadSpoolingManager("filesystem", filesystemProperties);
        return server;
    }

    private static String randomAES256Key()
    {
        return getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        minio.close();
        server.close();
        server = null;
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s?encoding=%s", server.getAddress(), getEncoding());
        return DriverManager.getConnection(url, "test", null);
    }

    protected abstract String getEncoding();
}
