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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCreateSchemaTask
{
    private static final String CATALOG_NAME = "catalog";
    private Session testSession;
    private TestCreateSchemaTask.MockMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        SchemaPropertyManager schemaPropertyManager = new SchemaPropertyManager();
        schemaPropertyManager.addProperties(testCatalog.getConnectorCatalogName(), ImmutableList.of());
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new TestCreateSchemaTask.MockMetadata(
                schemaPropertyManager,
                testCatalog.getConnectorCatalogName());
    }

    @Test
    public void testDuplicatedCreateSchema()
    {
        String schemaName = "test_db";
        CreateSchema statement = new CreateSchema(QualifiedName.of(schemaName), false, ImmutableList.of());
        getFutureValue(CreateSchemaTask.internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateSchemaCount(), 1);
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(CreateSchemaTask.internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList())))
                .withMessage("Schema already exists");
    }

    @Test
    public void testDuplicatedCreateSchemaIfNotExists()
    {
        String schemaName = "test_db";
        CreateSchema statement = new CreateSchema(QualifiedName.of(schemaName), true, ImmutableList.of());
        getFutureValue(CreateSchemaTask.internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateSchemaCount(), 1);
        getFutureValue(CreateSchemaTask.internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateSchemaCount(), 1);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final CatalogName catalogHandle;
        private final List<CatalogSchemaName> schemas;
        private final SchemaPropertyManager schemaPropertyManager;

        public MockMetadata(
                SchemaPropertyManager schemaPropertyManager,
                CatalogName catalogHandle)
        {
            this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.schemas = new CopyOnWriteArrayList<>();
        }

        @Override
        public SchemaPropertyManager getSchemaPropertyManager()
        {
            return schemaPropertyManager;
        }

        @Override
        public boolean schemaExists(Session session, CatalogSchemaName schema)
        {
            // To check the exception handling thrown by createSchema.
            return false;
        }

        @Override
        public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
        {
            if (schemas.contains(schema)) {
                throw new TrinoException(ALREADY_EXISTS, "Schema already exists");
            }
            schemas.add(schema);
        }

        @Override
        public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogHandle.getCatalogName().equals(catalogName)) {
                return Optional.of(catalogHandle);
            }
            return Optional.empty();
        }

        public int getCreateSchemaCount()
        {
            return schemas.size();
        }
    }
}
