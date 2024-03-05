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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Testcontainers
@TestInstance(PER_CLASS)
public class TestOpaAccessControlDataFilteringSystem
{
    @Container
    private static final OpaContainer OPA_CONTAINER = new OpaContainer();
    private static final String OPA_ALLOW_POLICY_NAME = "allow";
    private static final String OPA_ROW_LEVEL_FILTERING_POLICY_NAME = "rowFilters";
    private static final String OPA_COLUMN_MASKING_POLICY_NAME = "columnMask";
    private static final String SAMPLE_ROW_LEVEL_FILTERING_POLICY = """
            package trino
            import future.keywords.in
            import future.keywords.if
            import future.keywords.contains

            default allow := true

            table_resource := input.action.resource.table
            is_admin {
              input.context.identity.user == "admin"
            }

            rowFilters contains {"expression": "user_type <> 'customer'"} if {
                not is_admin
                table_resource.catalogName == "sample_catalog"
                table_resource.schemaName == "sample_schema"
                table_resource.tableName == "restricted_table"
            }""";
    private static final String SAMPLE_COLUMN_MASKING_POLICY = """
            package trino
            import future.keywords.in
            import future.keywords.if
            import future.keywords.contains

            default allow := true

            column_resource := input.action.resource.column
            is_admin {
              input.context.identity.user == "admin"
            }

            columnMask := {"expression": "NULL"} if {
                not is_admin
                column_resource.catalogName == "sample_catalog"
                column_resource.schemaName == "sample_schema"
                column_resource.tableName == "restricted_table"
                column_resource.columnName == "user_phone"
            }

            columnMask := {"expression": "'****' || substring(user_name, -3)"} if {
                not is_admin
                column_resource.catalogName == "sample_catalog"
                column_resource.schemaName == "sample_schema"
                column_resource.tableName == "restricted_table"
                column_resource.columnName == "user_name"
            }
            """;

    private static final Set<String> DUMMY_CUSTOMERS_IN_TABLE = ImmutableSet.of("customer_one", "customer_two");
    private static final Set<String> DUMMY_INTERNAL_USERS_IN_TABLE = ImmutableSet.of("some_internal_user");
    private static final Set<String> ALL_DUMMY_USERS_IN_TABLE = ImmutableSet.<String>builder()
            .addAll(DUMMY_INTERNAL_USERS_IN_TABLE)
            .addAll(DUMMY_CUSTOMERS_IN_TABLE)
            .build();

    private QueryRunnerHelper runner;

    @AfterEach
    public void teardown()
    {
        runner.teardown();
    }

    @Test
    public void testRowFilteringEnabled()
            throws Exception
    {
        setupTrinoWithOpa(
                new OpaConfig()
                        .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                        .setOpaRowFiltersUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ROW_LEVEL_FILTERING_POLICY_NAME)));
        OPA_CONTAINER.submitPolicy(SAMPLE_ROW_LEVEL_FILTERING_POLICY);
        @Language("SQL") String restrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.restricted_table";
        @Language("SQL") String unrestrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.unrestricted_table";
        assertResultsForUser("admin", restrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("admin", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);

        assertResultsForUser("bob", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("bob", restrictedTableQuery, DUMMY_INTERNAL_USERS_IN_TABLE);
    }

    @Test
    public void testRowFilteringDisabledDoesNothing()
            throws Exception
    {
        setupTrinoWithOpa(
                new OpaConfig()
                        .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME)));
        OPA_CONTAINER.submitPolicy(SAMPLE_ROW_LEVEL_FILTERING_POLICY);
        @Language("SQL") String restrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.restricted_table";
        @Language("SQL") String unrestrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.unrestricted_table";
        assertResultsForUser("admin", restrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("admin", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);

        assertResultsForUser("bob", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("bob", restrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
    }

    @Test
    public void testColumnMasking()
            throws Exception
    {
        setupTrinoWithOpa(
                new OpaConfig()
                        .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                        .setOpaColumnMaskingUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_COLUMN_MASKING_POLICY_NAME)));
        OPA_CONTAINER.submitPolicy(SAMPLE_COLUMN_MASKING_POLICY);

        @Language("SQL") String userNamesInUnrestrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.unrestricted_table";
        @Language("SQL") String userNamesInRestrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.restricted_table";
        // No masking is applied to the unrestricted table
        assertResultsForUser("admin", userNamesInUnrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("bob", userNamesInUnrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);

        // No masking is applied for "admin" even in the restricted table
        assertResultsForUser("admin", userNamesInRestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);

        // "bob" can only see the last 3 characters of user names for the restricted table
        Set<String> expectedMaskedUserNames = ALL_DUMMY_USERS_IN_TABLE.stream().map(userName -> "****" + userName.substring(userName.length() - 3)).collect(toImmutableSet());
        assertResultsForUser("bob", userNamesInRestrictedTableQuery, expectedMaskedUserNames);

        @Language("SQL") String phoneNumbersInUnrestrictedTableQuery = "SELECT user_phone FROM sample_catalog.sample_schema.unrestricted_table";
        @Language("SQL") String phoneNumbersInRestrictedTableQuery = "SELECT user_phone FROM sample_catalog.sample_schema.restricted_table";

        // Phone numbers are derived by hashing the name of the user
        Set<String> allExpectedPhoneNumbers = ALL_DUMMY_USERS_IN_TABLE.stream().map(userName -> String.valueOf(userName.hashCode())).collect(toImmutableSet());

        // No masking is applied to the unrestricted table
        assertResultsForUser("admin", phoneNumbersInUnrestrictedTableQuery, allExpectedPhoneNumbers);
        assertResultsForUser("bob", phoneNumbersInUnrestrictedTableQuery, allExpectedPhoneNumbers);

        // No masking is applied for "admin" even in the restricted table
        assertResultsForUser("admin", phoneNumbersInRestrictedTableQuery, allExpectedPhoneNumbers);
        // "bob" cannot see any phone numbers in the restricted table
        assertResultsForUser("bob", phoneNumbersInRestrictedTableQuery, ImmutableSet.of("<NULL>"));
    }

    @Test
    public void testColumnMaskingDisabledDoesNothing()
            throws Exception
    {
        setupTrinoWithOpa(new OpaConfig().setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME)));
        OPA_CONTAINER.submitPolicy(SAMPLE_COLUMN_MASKING_POLICY);
        @Language("SQL") String restrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.restricted_table";
        @Language("SQL") String unrestrictedTableQuery = "SELECT user_name FROM sample_catalog.sample_schema.unrestricted_table";
        assertResultsForUser("admin", restrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("admin", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);

        assertResultsForUser("bob", unrestrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("bob", restrictedTableQuery, ALL_DUMMY_USERS_IN_TABLE);
    }

    @Test
    public void testColumnMaskingAndRowFiltering()
            throws Exception
    {
        setupTrinoWithOpa(
                new OpaConfig()
                        .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                        .setOpaColumnMaskingUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_COLUMN_MASKING_POLICY_NAME))
                        .setOpaRowFiltersUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ROW_LEVEL_FILTERING_POLICY_NAME)));
        // Simpler policy than the previous tests:
        // Admin has no restrictions
        // Any other user can only see rows where "user_type" is not "customer"
        // And cannot see any data for field "user_name"
        String policy = """
                package trino
                import future.keywords.in
                import future.keywords.if
                import future.keywords.contains

                default allow := true

                is_admin {
                  input.context.identity.user == "admin"
                }

                table_resource := input.action.resource.table
                column_resource := input.action.resource.column

                rowFilters contains {"expression": "user_type <> 'customer'"} if {
                    not is_admin
                }
                columnMask := {"expression": "NULL"} if {
                    not is_admin
                    column_resource.columnName == "user_name"
                }""";
        OPA_CONTAINER.submitPolicy(policy);

        @Language("SQL") String selectUserNameData = "SELECT user_name FROM sample_catalog.sample_schema.restricted_table";
        @Language("SQL") String selectUserTypeData = "SELECT user_type FROM sample_catalog.sample_schema.restricted_table";
        Set<String> expectedUserTypes = ImmutableSet.of("internal_user", "customer");

        assertResultsForUser("admin", selectUserNameData, ALL_DUMMY_USERS_IN_TABLE);
        assertResultsForUser("admin", selectUserTypeData, expectedUserTypes);

        assertResultsForUser("bob", selectUserNameData, ImmutableSet.of("<NULL>"));
        assertResultsForUser("bob", selectUserTypeData, ImmutableSet.of("internal_user"));
    }

    private void assertResultsForUser(String asUser, @Language("SQL") String query, Set<String> expectedResults)
    {
        assertThat(runner.querySetOfStrings(asUser, query)).containsExactlyInAnyOrderElementsOf(expectedResults);
    }

    private void setupTrinoWithOpa(OpaConfig opaConfig)
    {
        this.runner = QueryRunnerHelper.withOpaConfig(opaConfig);
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("sample_schema"))
                .withListTables((session, schema) -> ImmutableList.<String>builder()
                        .add("restricted_table")
                        .add("unrestricted_table")
                        .build())
                .withGetColumns(schemaTableName -> ImmutableList.<ColumnMetadata>builder()
                        .add(ColumnMetadata.builder().setName("user_type").setType(VarcharType.VARCHAR).build())
                        .add(ColumnMetadata.builder().setName("user_name").setType(VarcharType.VARCHAR).build())
                        .add(ColumnMetadata.builder().setName("user_phone").setType(IntegerType.INTEGER).build())
                        .build())
                .withData(schemaTableName -> ImmutableList.<List<?>>builder()
                        .addAll(DUMMY_CUSTOMERS_IN_TABLE.stream().map(customer -> ImmutableList.of("customer", customer, customer.hashCode())).collect(toImmutableSet()))
                        .addAll(DUMMY_INTERNAL_USERS_IN_TABLE.stream().map(internalUser -> ImmutableList.of("internal_user", internalUser, internalUser.hashCode())).collect(toImmutableSet()))
                        .build())
                .build();

        runner.getBaseQueryRunner().installPlugin(new MockConnectorPlugin(connectorFactory));
        runner.getBaseQueryRunner().createCatalog("sample_catalog", "mock");
    }
}
