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
import com.google.inject.CreationException;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.client.NodeVersion;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TestMetadataManager;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.base.security.TestingSystemAccessControlContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Files.copy;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;

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
    private static final Instant queryStart = Instant.now();

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
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("admin-test"), "alice"))
                .isInstanceOf(AccessDeniedException.class);

        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("invalid"), "alice"))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("anything"), "test");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("invalid-other"), "test"))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("svc_tenant"), "svc_tenant_prod");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("svc_tenant"), "svc_tenant_other"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("svc_tenant"), "svc_other_prod"))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("external_corp_dept"), "internal-dept-corp-sandbox");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("external_corp_dept"), "internal-corp-dept-sandbox"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("external_corp_dept"), "invalid"))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanImpersonateUser(Identity.ofUser("missing_replacement_group"), "anything");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("incorrect_number_of_replacements_groups_group"), "$2_group_prod"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("new_user in impersonation rule refers to a capturing group that does not exist in original_user");
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.ofUser("incorrect_number_of_replacements_groups_group"), "group_group_prod"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("new_user in impersonation rule refers to a capturing group that does not exist in original_user");

        AccessControlManager accessControlManagerWithPrincipal = newAccessControlManager(transactionManager, "catalog_principal.json");
        accessControlManagerWithPrincipal.checkCanImpersonateUser(Identity.ofUser("anything"), "anythingElse");
    }

    @Test
    public void testDocsExample()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME);
        accessControlManager.loadSystemAccessControl(
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

        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(Optional.empty(), alice.getUser()))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControlManager.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser()))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser()))
                .isInstanceOf(AccessDeniedException.class);

        accessControlManager.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControlManager.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser()))
                .isInstanceOf(AccessDeniedException.class);

        AccessControlManager accessControlManagerNoPatterns = newAccessControlManager(transactionManager, "catalog.json");
        accessControlManagerNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testSystemInformation()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "system_information.json");

        accessControlManager.checkCanReadSystemInformation(admin);
        accessControlManager.checkCanWriteSystemInformation(admin);

        accessControlManager.checkCanReadSystemInformation(nonAsciiUser);
        accessControlManager.checkCanWriteSystemInformation(nonAsciiUser);

        accessControlManager.checkCanReadSystemInformation(admin);
        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(alice);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanReadSystemInformation(bob);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanWriteSystemInformation(bob);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, admin, queryId, queryStart), allCatalogs)).isEqualTo(allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed", "staff-catalog");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, alice, queryId, queryStart), allCatalogs)).isEqualTo(aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "staff-catalog");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, bob, queryId, queryStart), allCatalogs)).isEqualTo(bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, nonAsciiUser, queryId, queryStart), allCatalogs)).isEqualTo(nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testCatalogOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, admin, queryId, queryStart), allCatalogs)).isEqualTo(allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, alice, queryId, queryStart), allCatalogs)).isEqualTo(aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, bob, queryId, queryStart), allCatalogs)).isEqualTo(bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertThat(accessControlManager.filterCatalogs(new SecurityContext(transactionId, nonAsciiUser, queryId, queryStart), allCatalogs)).isEqualTo(nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testSchemaOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertThat(accessControlManager.filterSchemas(new SecurityContext(transactionId, alice, queryId, queryStart), "alice-catalog", aliceSchemas)).isEqualTo(aliceSchemas);
                    assertThat(accessControlManager.filterSchemas(new SecurityContext(transactionId, bob, queryId, queryStart), "alice-catalog", aliceSchemas)).isEqualTo(ImmutableSet.of());

                    accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema, ImmutableMap.of());
                    accessControlManager.checkCanDropSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema);
                    accessControlManager.checkCanRenameSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema, "new-schema");
                    accessControlManager.checkCanShowSchemas(new SecurityContext(transactionId, alice, queryId, queryStart), "alice-catalog");
                });
        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId, queryStart), aliceSchema, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testSchemaOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertThat(accessControlManager.filterSchemas(new SecurityContext(transactionId, alice, queryId, queryStart), "alice-catalog", aliceSchemas)).isEqualTo(aliceSchemas);
                    assertThat(accessControlManager.filterSchemas(new SecurityContext(transactionId, bob, queryId, queryStart), "alice-catalog", aliceSchemas)).isEqualTo(ImmutableSet.of());

                    accessControlManager.checkCanShowSchemas(new SecurityContext(transactionId, alice, queryId, queryStart), "alice-catalog");
                });

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create schema alice-catalog.schema");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop schema alice-catalog.schema");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(new SecurityContext(transactionId, alice, queryId, queryStart), aliceSchema, "new-schema");
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot rename schema from alice-catalog.schema to new-schema");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(new SecurityContext(transactionId, bob, queryId, queryStart), aliceSchema, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testTableOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId, queryStart);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId, queryStart);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId, queryStart);

                    assertThat(accessControlManager.filterTables(aliceContext, "alice-catalog", aliceTables)).isEqualTo(aliceTables);
                    assertThat(accessControlManager.filterTables(aliceContext, "staff-catalog", aliceTables)).isEqualTo(aliceTables);
                    assertThat(accessControlManager.filterTables(bobContext, "alice-catalog", aliceTables)).isEqualTo(ImmutableSet.of());
                    assertThat(accessControlManager.filterTables(bobContext, "staff-catalog", aliceTables)).isEqualTo(aliceTables);
                    assertThat(accessControlManager.filterTables(nonAsciiContext, "alice-catalog", aliceTables)).isEqualTo(ImmutableSet.of());
                    assertThat(accessControlManager.filterTables(nonAsciiContext, "staff-catalog", aliceTables)).isEqualTo(ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(aliceContext, aliceTable, Map.of());
                    accessControlManager.checkCanDropTable(aliceContext, aliceTable);
                    accessControlManager.checkCanTruncateTable(aliceContext, aliceTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, aliceTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, aliceTable);
                    accessControlManager.checkCanSetTableProperties(aliceContext, aliceTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(aliceContext, aliceTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, aliceTable);

                    accessControlManager.checkCanCreateTable(aliceContext, staffTable, Map.of());
                    accessControlManager.checkCanDropTable(aliceContext, staffTable);
                    accessControlManager.checkCanTruncateTable(aliceContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(aliceContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(aliceContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(aliceContext, staffTable);
                    accessControlManager.checkCanSetTableProperties(aliceContext, staffTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(aliceContext, staffTable);
                    accessControlManager.checkCanRenameColumn(aliceContext, staffTable);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(bobContext, aliceTable, Map.of()))
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

                    accessControlManager.checkCanCreateTable(bobContext, staffTable, Map.of());
                    accessControlManager.checkCanDropTable(bobContext, staffTable);
                    accessControlManager.checkCanTruncateTable(bobContext, staffTable);
                    accessControlManager.checkCanSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(bobContext, staffTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(bobContext, staffTable);
                    accessControlManager.checkCanDeleteFromTable(bobContext, staffTable);
                    accessControlManager.checkCanSetTableProperties(bobContext, staffTable, ImmutableMap.of());
                    accessControlManager.checkCanAddColumns(bobContext, staffTable);
                    accessControlManager.checkCanRenameColumn(bobContext, staffTable);

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(nonAsciiContext, aliceTable, Map.of()))
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

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(nonAsciiContext, staffTable, Map.of()))
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
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertThat(accessControlManager.filterTables(new SecurityContext(transactionId, alice, queryId, queryStart), "alice-catalog", aliceTables)).isEqualTo(aliceTables);
                    assertThat(accessControlManager.filterTables(new SecurityContext(transactionId, bob, queryId, queryStart), "alice-catalog", aliceTables)).isEqualTo(ImmutableSet.of());

                    accessControlManager.checkCanSelectFromColumns(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable, ImmutableSet.of());
                });

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable, Map.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropTable(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanTruncateTable(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot truncate table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanInsertIntoTable(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot insert into table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDeleteFromTable(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot delete from table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanSetTableProperties(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot set table properties to alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanAddColumns(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot add a column to table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameColumn(new SecurityContext(transactionId, alice, queryId, queryStart), aliceTable);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot rename a column in table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(new SecurityContext(transactionId, bob, queryId, queryStart), aliceTable, Map.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testViewOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId, queryStart);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId, queryStart);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId, queryStart);

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
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext context = new SecurityContext(transactionId, alice, queryId, queryStart);
                    accessControlManager.checkCanSelectFromColumns(context, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(context, "alice-catalog", "property");
                });

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create view alice-catalog.schema.view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop view alice-catalog.schema.view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanGrantTablePrivilege(new SecurityContext(transactionId, alice, queryId, queryStart), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot grant privilege SELECT on table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRevokeTablePrivilege(new SecurityContext(transactionId, alice, queryId, queryStart), SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot revoke privilege SELECT on table alice-catalog.schema.table");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(new SecurityContext(transactionId, bob, queryId, queryStart), aliceView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testMaterializedViewAccess()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext aliceContext = new SecurityContext(transactionId, alice, queryId, queryStart);
                    SecurityContext bobContext = new SecurityContext(transactionId, bob, queryId, queryStart);
                    SecurityContext nonAsciiContext = new SecurityContext(transactionId, nonAsciiUser, queryId, queryStart);

                    // User alice is allowed access to alice-catalog
                    accessControlManager.checkCanCreateMaterializedView(aliceContext, aliceMaterializedView, Map.of());
                    accessControlManager.checkCanDropMaterializedView(aliceContext, aliceMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(aliceContext, aliceMaterializedView);
                    accessControlManager.checkCanSetMaterializedViewProperties(aliceContext, aliceMaterializedView, ImmutableMap.of());

                    // User alice is part of staff group which is allowed access to staff-catalog
                    accessControlManager.checkCanCreateMaterializedView(aliceContext, staffMaterializedView, Map.of());
                    accessControlManager.checkCanDropMaterializedView(aliceContext, staffMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(aliceContext, staffMaterializedView);
                    accessControlManager.checkCanSetMaterializedViewProperties(aliceContext, staffMaterializedView, ImmutableMap.of());

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateMaterializedView(bobContext, aliceMaterializedView, Map.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropMaterializedView(bobContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRefreshMaterializedView(bobContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetMaterializedViewProperties(bobContext, aliceMaterializedView, ImmutableMap.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");

                    // User bob is part of staff group which is allowed access to staff-catalog
                    accessControlManager.checkCanCreateMaterializedView(bobContext, staffMaterializedView, Map.of());
                    accessControlManager.checkCanDropMaterializedView(bobContext, staffMaterializedView);
                    accessControlManager.checkCanRefreshMaterializedView(bobContext, staffMaterializedView);
                    accessControlManager.checkCanSetMaterializedViewProperties(bobContext, staffMaterializedView, ImmutableMap.of());

                    assertThatThrownBy(() -> accessControlManager.checkCanCreateMaterializedView(nonAsciiContext, aliceMaterializedView, Map.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanDropMaterializedView(nonAsciiContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanRefreshMaterializedView(nonAsciiContext, aliceMaterializedView))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                    assertThatThrownBy(() -> accessControlManager.checkCanSetMaterializedViewProperties(nonAsciiContext, aliceMaterializedView, ImmutableMap.of()))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage("Access Denied: Cannot access catalog alice-catalog");
                });
    }

    @Test
    public void testReadOnlyMaterializedViewAccess()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext context = new SecurityContext(transactionId, alice, queryId, queryStart);
                    accessControlManager.checkCanSelectFromColumns(context, aliceMaterializedView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(context, "alice-catalog", "property");
                });

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateMaterializedView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceMaterializedView, Map.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropMaterializedView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRefreshMaterializedView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot refresh materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanSetMaterializedViewProperties(new SecurityContext(transactionId, alice, queryId, queryStart), aliceMaterializedView, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot set properties of materialized view alice-catalog.schema.materialized-view");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateMaterializedView(new SecurityContext(transactionId, bob, queryId, queryStart), aliceMaterializedView, Map.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRefreshMaterializedView(new SecurityContext(transactionId, bob, queryId, queryStart), aliceMaterializedView);
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanSetMaterializedViewProperties(new SecurityContext(transactionId, bob, queryId, queryStart), aliceMaterializedView, ImmutableMap.of());
        })).isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot access catalog alice-catalog");
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = TestMetadataManager.builder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME);
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("catalog.json")), configFile);

        accessControlManager.loadSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of(
                SECURITY_CONFIG_FILE, configFile.getAbsolutePath(),
                SECURITY_REFRESH_PERIOD, "1ms"));

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                });

        copy(new File(getResourcePath("security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                }))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageStartingWith("Failed to convert JSON tree node");
        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                }))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageStartingWith("Failed to convert JSON tree node");

        copy(new File(getResourcePath("catalog.json")), configFile);
        sleep(2);

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(new SecurityContext(transactionId, alice, queryId, queryStart), aliceView);
                });
    }

    @Test
    public void testAllowModeIsRequired()
    {
        assertThatThrownBy(() -> newAccessControlManager(createTestTransactionManager(), "catalog_allow_unset.json"))
                .isInstanceOf(CreationException.class)
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    @Test
    public void testAllowModeInvalidValue()
    {
        assertThatThrownBy(() -> newAccessControlManager(createTestTransactionManager(), "catalog_invalid_allow_value.json"))
                .isInstanceOf(CreationException.class)
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME);

        accessControlManager.loadSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", getResourcePath(resourceName)));

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
        assertThatThrownBy(() -> parse(getResourcePath("security-config-file-with-unknown-rules.json")))
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    private void parse(String path)
    {
        new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, path), new TestingSystemAccessControlContext());
    }
}
