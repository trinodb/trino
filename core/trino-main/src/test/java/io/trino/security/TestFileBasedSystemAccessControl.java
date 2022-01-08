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
package io.trino.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Files.copy;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
import static org.testng.Assert.assertEquals;

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
    private static final Identity admin = Identity.forUser("alberto").withEnabledRoles(ImmutableSet.of("admin")).build();
    private static final Identity nonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withGroups(ImmutableSet.of("\u0194\u0194\u0194")).build();
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "\u0200\u0200\u0200", "staff-catalog");
    private static final QualifiedObjectName aliceTable = new QualifiedObjectName("alice-catalog", "schema", "table");
    private static final QualifiedObjectName aliceView = new QualifiedObjectName("alice-catalog", "schema", "view");
    private static final QualifiedObjectName aliceMaterializedView = new QualifiedObjectName("alice-catalog", "schema", "materialized-view");
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private static final QualifiedObjectName staffTable = new QualifiedObjectName("staff-catalog", "schema2", "table");
    private static final QualifiedObjectName staffView = new QualifiedObjectName("staff-catalog", "schema2", "view");
    private static final QualifiedObjectName staffMaterializedView = new QualifiedObjectName("staff-catalog", "schema2", "materialized-view");
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
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig(), DefaultSystemAccessControl.NAME);
        accessControlManager.setSystemAccessControl(
                FileBasedSystemAccessControl.NAME,
                ImmutableMap.of("security.config-file", new File("../../docs/src/main/sphinx/security/user-impersonation.json").getAbsolutePath()));

        accessControlManager.checkCanImpersonateUser(admin, "charlie");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(admin, "bob"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Access Denied: User alberto cannot impersonate user bob");

        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("charlie"), "doris"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Access Denied: User charlie cannot impersonate user doris");
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
        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(alice);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanReadSystemInformation(bob);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(bob);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, admin, queryId), allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed", "staff-catalog");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, alice, queryId), allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "staff-catalog");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, bob, queryId), allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, nonAsciiUser, queryId), allCatalogs), nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testCatalogOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, admin, queryId), allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, alice, queryId), allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, bob, queryId), allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(new SecurityContext(transactionId, nonAsciiUser, queryId), allCatalogs), nonAsciiUserCatalogs);
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
        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId), aliceSchema);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
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

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create schema alice-catalog.schema");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop schema alice-catalog.schema");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(new SecurityContext(transactionId, alice, queryId), aliceSchema, "new-schema");
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot rename schema from alice-catalog.schema to new-schema");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId), aliceSchema);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
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
                    accessControlManager.checkCanTruncateTable(aliceContext, aliceTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, aliceTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, aliceTable);
                    accessControlManager.checkCanSetTableProperties(aliceContext, aliceTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(aliceContext, aliceTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, aliceTable);

                    accessControlManager.checkCanCreateTable(aliceContext, staffTable);
                    accessControlManager.checkCanDropTable(aliceContext, staffTable);
                    accessControlManager.checkCanTruncateTable(aliceContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, staffTable);
                    accessControlManager.checkCanSetTableProperties(aliceContext, staffTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(aliceContext, staffTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, staffTable);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropTable(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanTruncateTable(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanInsertIntoTable(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDeleteFromTable(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetTableProperties(bobContext, aliceTable, ImmutableMap.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanAddColumns(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRenameColumn(bobContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    accessControlManager.checkCanCreateTable(bobContext, staffTable);
                    accessControlManager.checkCanDropTable(bobContext, staffTable);
                    accessControlManager.checkCanTruncateTable(bobContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(bobContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(bobContext, staffTable);
                    accessControlManager.checkCanSetTableProperties(bobContext, staffTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(bobContext, staffTable);
                    accessControlManager.checkCanRenameColumn(bobContext, staffTable);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropTable(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanTruncateTable(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanInsertIntoTable(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDeleteFromTable(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetTableProperties(nonAsciiContext, aliceTable, ImmutableMap.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanAddColumns(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRenameColumn(nonAsciiContext, aliceTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropTable(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanTruncateTable(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanInsertIntoTable(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDeleteFromTable(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetTableProperties(nonAsciiContext, staffTable, ImmutableMap.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanAddColumns(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRenameColumn(nonAsciiContext, staffTable))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
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

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanTruncateTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot truncate table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanInsertIntoTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot insert into table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDeleteFromTable(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot delete from table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanSetTableProperties(new SecurityContext(transactionId, alice, queryId), aliceTable, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot set table properties to alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanAddColumns(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot add a column to table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameColumn(new SecurityContext(transactionId, alice, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot rename a column in table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, bob, queryId), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
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
                    accessControlManager.checkCanGrantTablePrivilege(aliceContext, SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(aliceContext, SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true);

                    accessControlManager.checkCanCreateView(aliceContext, staffView);
                    accessControlManager.checkCanDropView(aliceContext, staffView);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(aliceContext, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(aliceContext, SELECT, staffTable, new TrinoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(aliceContext, SELECT, staffTable, new TrinoPrincipal(USER, "revokee"), true);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateView(bobContext, aliceView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropView(bobContext, aliceView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(bobContext, aliceView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, aliceView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetCatalogSessionProperty(bobContext, "alice-catalog", "property"))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanGrantTablePrivilege(bobContext, SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRevokeTablePrivilege(bobContext, SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    accessControlManager.checkCanCreateView(bobContext, staffView);
                    accessControlManager.checkCanDropView(bobContext, staffView);
                    accessControlManager.checkCanSelectFromColumns(bobContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(bobContext, "staff-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(bobContext, SELECT, staffTable, new TrinoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(bobContext, SELECT, staffTable, new TrinoPrincipal(USER, "revokee"), true);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateView(nonAsciiContext, aliceView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropView(nonAsciiContext, aliceView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, aliceView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, aliceView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetCatalogSessionProperty(nonAsciiContext, "alice-catalog", "property"))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanGrantTablePrivilege(nonAsciiContext, SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRevokeTablePrivilege(nonAsciiContext, SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateView(nonAsciiContext, staffView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropView(nonAsciiContext, staffView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(nonAsciiContext, staffView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffTable, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(nonAsciiContext, staffView, ImmutableSet.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetCatalogSessionProperty(nonAsciiContext, "staff-catalog", "property"))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanGrantTablePrivilege(nonAsciiContext, SELECT, staffTable, new TrinoPrincipal(USER, "grantee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRevokeTablePrivilege(nonAsciiContext, SELECT, staffTable, new TrinoPrincipal(USER, "revokee"), true))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog staff-catalog");
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

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create view alice-catalog.schema.view");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropView(new SecurityContext(transactionId, alice, queryId), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop view alice-catalog.schema.view");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanGrantTablePrivilege(new SecurityContext(transactionId, alice, queryId), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot grant privilege SELECT on table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRevokeTablePrivilege(new SecurityContext(transactionId, alice, queryId), SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot revoke privilege SELECT on table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, bob, queryId), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testMaterializedViewAccess()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId);

                    // User alice is allowed access to alice-catalog
                    accessControlManager.checkCanCreateMaterializedView(aliceContext, aliceMaterializedView);
                    accessControlManager.checkCanDropMaterializedView(aliceContext, aliceMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(aliceContext, aliceMaterializedView);

                    // User alice is part of staff group which is allowed access to staff-catalog
                    accessControlManager.checkCanCreateMaterializedView(aliceContext, staffMaterializedView);
                    accessControlManager.checkCanDropMaterializedView(aliceContext, staffMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(aliceContext, staffMaterializedView);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateMaterializedView(bobContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropMaterializedView(bobContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRefreshMaterializedView(bobContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    // User bob is part of staff group which is allowed access to staff-catalog
                    accessControlManager.checkCanCreateMaterializedView(bobContext, staffMaterializedView);
                    accessControlManager.checkCanDropMaterializedView(bobContext, staffMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(bobContext, staffMaterializedView);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateMaterializedView(nonAsciiContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropMaterializedView(nonAsciiContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRefreshMaterializedView(nonAsciiContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                });
    }

    @Test
    public void testReadOnlyMaterializedViewAccess()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext context = new SecurityContext(transactionId, alice, queryId);
                    accessControlManager.checkCanSelectFromColumns(context, aliceMaterializedView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(context, "alice-catalog", "property");
                });

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateMaterializedView(new SecurityContext(transactionId, alice, queryId), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropMaterializedView(new SecurityContext(transactionId, alice, queryId), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRefreshMaterializedView(new SecurityContext(transactionId, alice, queryId), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot refresh materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateMaterializedView(new SecurityContext(transactionId, bob, queryId), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRefreshMaterializedView(new SecurityContext(transactionId, bob, queryId), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig(), DefaultSystemAccessControl.NAME);
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
        assertThatThrownBy(() -> newAccessControlManager(createTestTransactionManager(), "catalog_allow_unset.json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");
    }

    @Test
    public void testAllowModeInvalidValue()
    {
        assertThatThrownBy(() -> newAccessControlManager(createTestTransactionManager(), "catalog_invalid_allow_value.json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig(), DefaultSystemAccessControl.NAME);

        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", getResourcePath(resourceName)));

        return accessControlManager;
    }

    private String getResourcePath(String resourceName)
    {
        try {
            return new File(getResource(resourceName).toURI()).getPath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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
