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
package io.trino.plugin.deltalake;

import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeSchemaComments
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .setWorkerCount(0)
                .setMetastore(new SchemaCommentMetastore())
                .build();
    }

    @Test
    void testSchemaComment()
    {
        assertSchemaComment(Optional.of("schema comment"));
    }

    @Test
    void testEmptySchemaComment()
    {
        assertSchemaComment(Optional.of(""));
    }

    @Test
    void testAbsentSchemaComment()
    {
        assertSchemaComment(Optional.empty());
    }

    private void assertSchemaComment(Optional<String> comment)
    {
        HiveMetastore metastore = getConnectorService(getQueryRunner(), HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        String schema = "test_schema_comment_" + randomNameSuffix();
        metastore.createDatabase(Database.builder()
                .setDatabaseName(schema)
                .setOwnerName(Optional.of(getSession().getUser()))
                .setOwnerType(Optional.of(USER))
                .setComment(comment)
                .build());
        try {
            assertThat(computeScalar("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'delta' AND schema_name = '%s'".formatted(schema)))
                    .isEqualTo(comment.orElse(null));
        }
        finally {
            metastore.dropDatabase(schema, false);
        }
    }

    @Test
    void testGetSchemaCommentForMissingSchema()
    {
        DeltaLakeMetadata metadata = getConnectorService(getQueryRunner(), DeltaLakeMetadataFactory.class)
                .create(SESSION.getIdentity());
        try {
            assertThatThrownBy(() -> metadata.getSchemaComment(SESSION, "missing_schema"))
                    .isInstanceOf(SchemaNotFoundException.class);
        }
        finally {
            metadata.cleanupQuery(SESSION);
        }
    }

    @Test
    void testGetSchemaCommentForSystemSchema()
    {
        DeltaLakeMetadata metadata = getConnectorService(getQueryRunner(), DeltaLakeMetadataFactory.class)
                .create(SESSION.getIdentity());
        try {
            assertThat(metadata.getSchemaComment(SESSION, "information_schema")).isEmpty();
            assertThat(metadata.getSchemaComment(SESSION, "sys")).isEmpty();
        }
        finally {
            metadata.cleanupQuery(SESSION);
        }
    }

    private static class SchemaCommentMetastore
            extends FileHiveMetastore
    {
        // The file metastore does not persist descriptions. Preserve them here to model HMS database responses.
        private final Map<String, Optional<String>> comments = new HashMap<>();

        public SchemaCommentMetastore()
        {
            super(new NodeVersion("test"),
                    new MemoryFileSystemFactory(),
                    false,
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory("memory:///")
                            .setMetastoreUser("test"));
        }

        @Override
        public synchronized void createDatabase(Database database)
        {
            super.createDatabase(database);
            comments.put(database.getDatabaseName(), database.getComment());
        }

        @Override
        public synchronized Optional<Database> getDatabase(String databaseName)
        {
            return super.getDatabase(databaseName)
                    .map(database -> Database.builder(database)
                            .setComment(comments.getOrDefault(databaseName, Optional.empty()))
                            .build());
        }

        @Override
        public synchronized void dropDatabase(String databaseName, boolean deleteData)
        {
            super.dropDatabase(databaseName, deleteData);
            comments.remove(databaseName);
        }
    }
}
