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
package io.prestosql.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Files.copy;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.security.Privilege.SELECT;
import static io.prestosql.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestFileBasedSystemAccessControl
{
    private static final Identity alice = Identity.forUser("alice").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity kerberosValidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidNonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withPrincipal(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInvalidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexWildDot = Identity.forUser(".*").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexEndQuote = Identity.forUser("\\E").withPrincipal(new KerberosPrincipal("special/\\E@EXAMPLE.COM")).build();
    private static final Identity invalidSpecialRegex = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.forUser("bob").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity admin = Identity.forUser("admin").withGroups(ImmutableSet.of("admin")).build();
    private static final Identity nonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withGroups(ImmutableSet.of("\u0194\u0194\u0194")).build();
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "\u0200\u0200\u0200", "staff-catalog");
    private static final QualifiedObjectName aliceTable = new QualifiedObjectName("alice-catalog", "schema", "table");
    private static final QualifiedObjectName aliceView = new QualifiedObjectName("alice-catalog", "schema", "view");
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private static final QualifiedObjectName staffTable = new QualifiedObjectName("staff-catalog", "schema2", "table");
    private static final QualifiedObjectName staffView = new QualifiedObjectName("staff-catalog", "schema2", "view");
    private static final QueryId queryId = new QueryId("query_id");

    @Test
    public void testCanImpersonateUserOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_impersonation.json");

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("alice"), "bob");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("alice"), "charlie");
        try {
            accessControlManager.checkCanImpersonateUser(Identity.ofUser("alice"), "admin");
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin"), "alice");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin"), "bob");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin"), "anything");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin-other"), "anything");
        try {
            accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin-test"), "alice");
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        try {
            accessControlManager.checkCanImpersonateUser(Identity.ofUser("invalid"), "alice");
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("anything"), "test");
        try {
            accessControlManager.checkCanImpersonateUser(Identity.ofUser("invalid-other"), "test");
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager = newAccessControlManager(transactionManager, "catalog_principal.json");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("anything"), "anythingElse");
    }

    @Test
    public void testDocsExample()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig());
        accessControlManager.setSystemAccessControl(
                FileBasedSystemAccessControl.NAME,
                ImmutableMap.of("security.config-file", new File("../presto-docs/src/main/sphinx/security/user-impersonation.json").getAbsolutePath()));

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("alice"), "charlie");
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("bob"), "charlie");
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("alice"), "bob"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("bob"), "alice"));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("charlie"), "doris"));
        accessControlManager.checkCanImpersonateUser(Identity.ofUser("charlie"), "test");
    }

    @Test
    public void testCanSetUserOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_principal.json");

        try {
            accessControlManager.checkCanSetUser(Optional.empty(), alice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControlManager.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControlManager.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        try {
            accessControlManager.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        AccessControlManager accessControlManagerNoPatterns = newAccessControlManager(transactionManager, "catalog.json");
        accessControlManagerNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testSystemInformation()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "system_information.json");

        accessControlManager.checkCanReadSystemInformation(admin);
        accessControlManager.checkCanWriteSystemInformation(admin);

        accessControlManager.checkCanReadSystemInformation(nonAsciiUser);
        accessControlManager.checkCanWriteSystemInformation(nonAsciiUser);

        accessControlManager.checkCanReadSystemInformation(admin);
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(alice);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanReadSystemInformation(bob);
        }));
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(bob);
        }));
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(admin, allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed", "staff-catalog");
                    assertEquals(accessControlManager.filterCatalogs(alice, allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "staff-catalog");
                    assertEquals(accessControlManager.filterCatalogs(bob, allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(nonAsciiUser, allCatalogs), nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testCatalogOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(admin, allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(alice, allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(bob, allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(nonAsciiUser, allCatalogs), nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testSchemaOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(new SecurityContext(transactionId, alice, queryId), "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(new SecurityContext(transactionId, bob, queryId), "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
                    accessControlManager.checkCanDropSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
                    accessControlManager.checkCanRenameSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema, "new-schema");
                    accessControlManager.checkCanShowSchemas(new SecurityContext(transactionId, alice, queryId), "alice-catalog");
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId), aliceSchema);
        }));
    }

    @Test
    public void testSchemaOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(new SecurityContext(transactionId, alice, queryId), "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(new SecurityContext(transactionId, bob, queryId), "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanShowSchemas(new SecurityContext(transactionId, alice, queryId), "alice-catalog");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema, "new-schema");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId), aliceSchema);
        }));
    }

    @Test
    public void testTableOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId);

                    assertEquals(accessControlManager.filterTables(aliceContext, "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(aliceContext, "staff-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(bobContext, "alice-catalog", aliceTables), ImmutableSet.of());
                    assertEquals(accessControlManager.filterTables(bobContext, "staff-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(nonAsciiContext, "alice-catalog", aliceTables), ImmutableSet.of());
                    assertEquals(accessControlManager.filterTables(nonAsciiContext, "staff-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(aliceContext, aliceTable);
                    accessControlManager.checkCanDropTable(aliceContext, aliceTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, aliceTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, aliceTable);
                    accessControlManager.checkCanAddColumns(aliceContext, aliceTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, aliceTable);

                    accessControlManager.checkCanCreateTable(aliceContext, staffTable);
                    accessControlManager.checkCanDropTable(aliceContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, staffTable);
                    accessControlManager.checkCanAddColumns(aliceContext, staffTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, staffTable);

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateTable(bobContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropTable(bobContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanInsertIntoTable(bobContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDeleteFromTable(bobContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanAddColumns(bobContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameColumn(bobContext, aliceTable));

                    accessControlManager.checkCanCreateTable(bobContext, staffTable);
                    accessControlManager.checkCanDropTable(bobContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(bobContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(bobContext, staffTable);
                    accessControlManager.checkCanAddColumns(bobContext, staffTable);
                    accessControlManager.checkCanRenameColumn(bobContext, staffTable);

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateTable(nonAsciiContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropTable(nonAsciiContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanInsertIntoTable(nonAsciiContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDeleteFromTable(nonAsciiContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanAddColumns(nonAsciiContext, aliceTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameColumn(nonAsciiContext, aliceTable));

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateTable(nonAsciiContext, staffTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropTable(nonAsciiContext, staffTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanInsertIntoTable(nonAsciiContext, staffTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDeleteFromTable(nonAsciiContext, staffTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanAddColumns(nonAsciiContext, staffTable));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameColumn(nonAsciiContext, staffTable));
                });
    }

    @Test
    public void testTableOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(new SecurityContext(transactionId, alice, queryId), "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(new SecurityContext(transactionId, bob, queryId), "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanSelectFromColumns(new SecurityContext(transactionId, alice, queryId), aliceTable, ImmutableSet.of());
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanInsertIntoTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDeleteFromTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanAddColumns(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameColumn(new SecurityContext(transactionId, alice, queryId), aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, bob, queryId), aliceTable);
        }));
    }

    @Test
    public void testViewOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId);

                    accessControlManager.checkCanCreateView(aliceContext, aliceView);
                    accessControlManager.checkCanDropView(aliceContext, aliceView);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(aliceContext, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(aliceContext, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(aliceContext, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true);

                    accessControlManager.checkCanCreateView(aliceContext, staffView);
                    accessControlManager.checkCanDropView(aliceContext, staffView);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(aliceContext, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(aliceContext, SELECT, staffTable, new PrestoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(aliceContext, SELECT, staffTable, new PrestoPrincipal(USER, "revokee"), true);

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateView(bobContext, aliceView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropView(bobContext, aliceView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(bobContext, aliceView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetCatalogSessionProperty(bobContext, "alice-catalog", "property"));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantTablePrivilege(bobContext, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeTablePrivilege(bobContext, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true));

                    accessControlManager.checkCanCreateView(bobContext, staffView);
                    accessControlManager.checkCanDropView(bobContext, staffView);
                    accessControlManager.checkCanSelectFromColumns(bobContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(bobContext, "staff-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(bobContext, SELECT, staffTable, new PrestoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(bobContext, SELECT, staffTable, new PrestoPrincipal(USER, "revokee"), true);

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateView(nonAsciiContext, aliceView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropView(nonAsciiContext, aliceView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, aliceView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetCatalogSessionProperty(nonAsciiContext, "alice-catalog", "property"));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantTablePrivilege(nonAsciiContext, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeTablePrivilege(nonAsciiContext, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true));

                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateView(nonAsciiContext, staffView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropView(nonAsciiContext, staffView));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, staffView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffView, ImmutableSet.of()));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetCatalogSessionProperty(nonAsciiContext, "staff-catalog", "property"));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantTablePrivilege(nonAsciiContext, SELECT, staffTable, new PrestoPrincipal(USER, "grantee"), true));
                    assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeTablePrivilege(nonAsciiContext, SELECT, staffTable, new PrestoPrincipal(USER, "revokee"), true));
                });
    }

    @Test
    public void testViewOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext context = new SecurityContext(transactionId, alice, queryId);
                    accessControlManager.checkCanSelectFromColumns(context, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(context, "alice-catalog", "property");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropView(new SecurityContext(transactionId, alice, queryId), aliceView);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanGrantTablePrivilege(new SecurityContext(transactionId, alice, queryId), SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRevokeTablePrivilege(new SecurityContext(transactionId, alice, queryId), SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, bob, queryId), aliceView);
        }));
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig());
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("catalog.json")), configFile);

        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of(
                SECURITY_CONFIG_FILE, configFile.getAbsolutePath(),
                SECURITY_REFRESH_PERIOD, "1ms"));

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                });

        copy(new File(getResourcePath("security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");
        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        copy(new File(getResourcePath("catalog.json")), configFile);
        sleep(2);

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
                });
    }

    @Test
    public void testAllowModeIsRequired()
    {
        assertThrows(IllegalArgumentException.class, () -> newAccessControlManager(createTestTransactionManager(), "catalog_allow_unset.json"));
    }

    @Test
    public void testAllowModeInvalidValue()
    {
        assertThrows(IllegalArgumentException.class, () -> newAccessControlManager(createTestTransactionManager(), "catalog_invalid_allow_value.json"));
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig());

        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", getResourcePath(resourceName)));

        return accessControlManager;
    }

    private String getResourcePath(String resourceName)
    {
        return this.getClass().getClassLoader().getResource(resourceName).getPath();
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> parse("src/test/resources/security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Invalid JSON");
    }

    private void parse(String path)
    {
        new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, path));
    }
}
