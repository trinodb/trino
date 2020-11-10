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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.Connector;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.PrincipalSpecification;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.sql.tree.PrincipalSpecification.Type.ROLE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGrantTask
{
    @Test
    public void testGrantToNonExistingRole()
    {
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("test"));
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Session testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .setCatalog("test")
                .build();
        MetadataManager metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        AccessControl accessControl = new AllowAllAccessControl();

        Grant statement = new Grant(Optional.empty(), Optional.empty(), QualifiedName.of("test.schema.ignore"), new PrincipalSpecification(ROLE, new Identifier("non-existent")), false);

        assertThatThrownBy(() -> new GrantTask().execute(statement, transactionManager, metadata, accessControl, testSession, ImmutableList.of()))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Role 'non-existent' does not exist");
    }

    public static Catalog createBogusTestingCatalog(String catalogName)
    {
        Connector connector = MockConnectorFactory.builder().build()
                .create(catalogName, null, null);
        CatalogName catalog = new CatalogName(catalogName);
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                connector,
                createSystemTablesCatalogName(catalog),
                connector);
    }
}
