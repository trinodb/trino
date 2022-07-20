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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.SELECT;

public class RangerSystemAccessControlTest
{
    static RangerSystemAccessControlImpl accessControlManager;

    private static final Identity alice = Identity.ofUser("alice");
    private static final Identity admin = Identity.ofUser("admin");
    //private static final Identity aliceWithGroups = Identity.from(alice).withGroups(new HashSet(Arrays.asList("users", "friends"))).build();
    //private static final Identity kerberosValidAlice = Identity.from(alice).withPrincipal(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")).build();
    //private static final Identity kerberosValidNonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withPrincipal(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInvalidAlice = Identity.from(alice).withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.ofUser("bob");
    //private static final Identity nonAsciiUser = Identity.ofUser("\u0194\u0194\u0194");

    private static final Set<String> allCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "alice-catalog");
    private static final Set<String> queryOwners = ImmutableSet.of("bob", "alice", "frank");
    private static final String aliceCatalog = "alice-catalog";
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private static final CatalogSchemaTableName aliceTable = new CatalogSchemaTableName("alice-catalog", "schema", "table");
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");

    private static final CatalogSchemaRoutineName aliceProcedure = new CatalogSchemaRoutineName("alice-catalog", "schema", "procedure");
    private static final String functionName = new String("function");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Map<String, String> config = new HashMap<>();
        accessControlManager = new RangerSystemAccessControlImpl(config);
    }

    @Test
    @SuppressWarnings("PMD")
    public void testCanSetUserOperations()
    {
        try {
            accessControlManager.checkCanImpersonateUser(context(alice), bob.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanImpersonateUser(context(admin), bob.getUser());

        try {
            accessControlManager.checkCanImpersonateUser(context(kerberosInvalidAlice), bob.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }
    }

    @Test
    public void testCatalogOperations()
    {
        Assert.assertEquals(accessControlManager.filterCatalogs(context(alice), allCatalogs), allCatalogs);
        Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
        Assert.assertEquals(accessControlManager.filterCatalogs(context(bob), allCatalogs), bobCatalogs);
        //Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
        //assertEquals(accessControlManager.filterCatalogs(context(nonAsciiUser), allCatalogs), nonAsciiUserCatalogs);
    }

    @Test
    @SuppressWarnings("PMD")
    public void testSchemaOperations()
    {
        Set<String> aliceSchemas = ImmutableSet.of("schema");
        Assert.assertEquals(accessControlManager.filterSchemas(context(alice), aliceCatalog, aliceSchemas), aliceSchemas);
        Assert.assertEquals(accessControlManager.filterSchemas(context(bob), "alice-catalog", aliceSchemas), ImmutableSet.of());

        accessControlManager.checkCanCreateSchema(context(alice), aliceSchema);
        accessControlManager.checkCanDropSchema(context(alice), aliceSchema);
        accessControlManager.checkCanRenameSchema(context(alice), aliceSchema, "new-schema");
        accessControlManager.checkCanShowSchemas(context(alice), aliceCatalog);

        try {
            accessControlManager.checkCanCreateSchema(context(bob), aliceSchema);
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetSchemaAuthorization(context(alice), aliceSchema, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanShowCreateSchema(context(alice), aliceSchema);
    }

    @Test
    @SuppressWarnings("PMD")
    public void testTableOperations()
    {
        Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
        Assert.assertEquals(accessControlManager.filterTables(context(alice), aliceCatalog, aliceTables), aliceTables);
        Assert.assertEquals(accessControlManager.filterTables(context(bob), "alice-catalog", aliceTables), ImmutableSet.of());

        accessControlManager.checkCanCreateTable(context(alice), aliceTable, Map.of());
        accessControlManager.checkCanDropTable(context(alice), aliceTable);
        accessControlManager.checkCanSelectFromColumns(context(alice), aliceTable, ImmutableSet.of());
        accessControlManager.checkCanInsertIntoTable(context(alice), aliceTable);
        accessControlManager.checkCanDeleteFromTable(context(alice), aliceTable);
        accessControlManager.checkCanRenameColumn(context(alice), aliceTable);

        try {
            accessControlManager.checkCanCreateTable(context(bob), aliceTable, Map.of());
        }
        catch (AccessDeniedException expected) {
        }
    }

    @Test
    @SuppressWarnings("PMD")
    public void testViewOperations()
    {
        accessControlManager.checkCanCreateView(context(alice), aliceView);
        accessControlManager.checkCanDropView(context(alice), aliceView);
        accessControlManager.checkCanSelectFromColumns(context(alice), aliceView, ImmutableSet.of());
        accessControlManager.checkCanCreateViewWithSelectFromColumns(context(alice), aliceTable, ImmutableSet.of());
        accessControlManager.checkCanCreateViewWithSelectFromColumns(context(alice), aliceView, ImmutableSet.of());
        accessControlManager.checkCanSetCatalogSessionProperty(context(alice), aliceCatalog, "property");
        accessControlManager.checkCanGrantTablePrivilege(context(alice), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true);
        accessControlManager.checkCanRevokeTablePrivilege(context(alice), SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true);

        try {
            accessControlManager.checkCanCreateView(context(bob), aliceView);
        }
        catch (AccessDeniedException expected) {
        }
    }

    @Test
    @SuppressWarnings("PMD")
    public void testMisc()
    {
        Assert.assertEquals(accessControlManager.filterViewQueryOwnedBy(context(alice), queryOwners), queryOwners);

        // check {type} / {col} replacement
        final VarcharType varcharType = VarcharType.createVarcharType(20);

        List<ViewExpression> ret = accessControlManager.getColumnMasks(context(alice), aliceTable, "cast_me", varcharType);
        Assert.assertNotNull(ret.get(0));
        Assert.assertEquals(ret.get(0).getExpression(), "cast cast_me as varchar(20)");

        ret = accessControlManager.getColumnMasks(context(alice), aliceTable, "do-not-cast-me", varcharType);
        Assert.assertEquals(ret.size(), 0);

        ret = accessControlManager.getRowFilters(context(alice), aliceTable);
        Assert.assertEquals(ret.size(), 0);

        accessControlManager.checkCanExecuteFunction(context(alice), functionName);
        accessControlManager.checkCanGrantExecuteFunctionPrivilege(context(alice), functionName, new TrinoPrincipal(USER, "grantee"), true);
        accessControlManager.checkCanExecuteProcedure(context(alice), aliceProcedure);
    }

    private SystemSecurityContext context(Identity id)
    {
        return new SystemSecurityContext(id, Optional.empty());
    }
}
