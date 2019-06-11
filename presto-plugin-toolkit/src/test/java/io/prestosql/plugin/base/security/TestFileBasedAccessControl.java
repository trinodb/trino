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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertThrows;

public class TestFileBasedAccessControl
{
    @Test
    public void testSchemaRules()
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");

        accessControl.checkCanCreateSchema(user("admin"), "test");
        accessControl.checkCanCreateSchema(user("bob"), "bob");
        assertDenied(() -> accessControl.checkCanCreateSchema(user("bob"), "test"));

        accessControl.checkCanDropSchema(user("admin"), "test");
        accessControl.checkCanDropSchema(user("bob"), "bob");
        assertDenied(() -> accessControl.checkCanDropSchema(user("bob"), "test"));

        accessControl.checkCanRenameSchema(user("admin"), "test", "new_schema");
        assertDenied(() -> accessControl.checkCanRenameSchema(user("bob"), "test", "new_schema"));
        assertDenied(() -> accessControl.checkCanRenameSchema(user("bob"), "bob", "new_schema"));

        accessControl.checkCanCreateTable(user("admin"), new SchemaTableName("test", "test"));
        accessControl.checkCanCreateTable(user("bob"), new SchemaTableName("bob", "test"));
        assertDenied(() -> accessControl.checkCanCreateTable(user("bob"), new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanCreateTable(user("admin"), new SchemaTableName("secret", "test")));
    }

    @Test
    public void testTableRules()
    {
        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromColumns(user("alice"), new SchemaTableName("test", "test"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(user("alice"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(user("alice"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of("bobcolumn"));
        accessControl.checkCanSelectFromColumns(user("bob"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanInsertIntoTable(user("bob"), new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanDeleteFromTable(user("bob"), new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanSelectFromColumns(user("joe"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanCreateViewWithSelectFromColumns(user("bob"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanDropTable(user("admin"), new SchemaTableName("bobschema", "bobtable"));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(user("alice"), new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanDropTable(user("bob"), new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(user("bob"), new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(user("admin"), new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(user("joe"), new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromColumns(user("joe"), new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of()));
    }

    @Test
    public void testSessionPropertyRules()
    {
        ConnectorAccessControl accessControl = createAccessControl("session_property.json");
        accessControl.checkCanSetCatalogSessionProperty(user("admin"), "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(user("alice"), "safe");
        accessControl.checkCanSetCatalogSessionProperty(user("alice"), "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(user("bob"), "safe");
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(user("bob"), "unsafe"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(user("alice"), "dangerous"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(user("charlie"), "safe"));
    }

    @Test
    public void testInvalidRules()
    {
        assertThatThrownBy(() -> createAccessControl("invalid.json"))
                .hasMessageContaining("Invalid JSON");
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, FileBasedAccessControl.class);
    }

    private static ConnectorSecurityContext user(String name)
    {
        return new ConnectorSecurityContext()
        {
            @Override
            public ConnectorTransactionHandle getTransactionHandle()
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
            }
        };
    }

    private ConnectorAccessControl createAccessControl(String fileName)
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        FileBasedAccessControlConfig config = new FileBasedAccessControlConfig();
        config.setConfigFile(path);
        return new FileBasedAccessControl(config);
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
