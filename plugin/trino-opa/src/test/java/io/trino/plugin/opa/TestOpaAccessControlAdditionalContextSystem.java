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

import io.trino.plugin.blackhole.BlackHolePlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Testcontainers
@TestInstance(PER_CLASS)
final class TestOpaAccessControlAdditionalContextSystem
{
    private static final String OPA_ALLOW_POLICY_NAME = "allow";
    private static final String OPA_BATCH_ALLOW_POLICY_NAME = "batchAllow";
    private static final File OPA_ADDITIONAL_CONTEXT_FILE = new File("src/test/resources/additional-context.properties");
    @Container
    private static final OpaContainer OPA_CONTAINER = new OpaContainer();

    @Test
    void testAllowsQueryAndFilters()
            throws Exception
    {
        QueryRunnerHelper runner = setupTrinoWithOpa(new OpaConfig()
                .setAdditionalContextFile(OPA_ADDITIONAL_CONTEXT_FILE)
                .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME)));

        OPA_CONTAINER.submitPolicy(
                """
                package trino
                import future.keywords.if
                import future.keywords.in

                default allow = false
                allow if is_admin
                allow {
                  is_bob
                  is_some_namespace
                  can_be_accessed_by_bob
                }

                is_admin {
                  input.context.identity.user == "admin"
                }
                is_bob {
                  input.context.identity.user == "bob"
                }
                is_some_namespace {
                  input.context.properties.namespace == "some-namespace"
                }
                can_be_accessed_by_bob {
                  input.action.operation in ["ImpersonateUser", "ExecuteQuery"]
                }
                can_be_accessed_by_bob {
                  input.action.operation in ["FilterCatalogs", "AccessCatalog"]
                  input.action.resource.catalog.name == "catalog_one"
                }
                """);
        Set<String> catalogsForBob = runner.querySetOfStrings("bob", "SHOW CATALOGS");
        assertThat(catalogsForBob).containsExactlyInAnyOrder("catalog_one");
        Set<String> catalogsForAdmin = runner.querySetOfStrings("admin", "SHOW CATALOGS");
        assertThat(catalogsForAdmin).containsExactlyInAnyOrder("catalog_one", "catalog_two", "system");
    }

    @Test
    void testShouldDenyQueryIfDirected()
            throws Exception
    {
        QueryRunnerHelper runner = setupTrinoWithOpa(new OpaConfig()
                .setAdditionalContextFile(OPA_ADDITIONAL_CONTEXT_FILE)
                .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME)));

        OPA_CONTAINER.submitPolicy(
                """
                package trino
                import future.keywords.in
                default allow = false

                allow {
                  input.context.identity.user == "admin"
                }

                allow {
                  input.context.identity.user == "someone"
                  input.context.properties.namespace == "diff-namespace"
                }
                """);
        assertThatThrownBy(() -> runner.querySetOfStrings("someone", "SHOW CATALOGS"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Access Denied");
        // smoke test: we can still query if we are the right user
        runner.querySetOfStrings("admin", "SHOW CATALOGS");
    }

    @Test
    void testFilterOutItemsBatch()
            throws Exception
    {
        QueryRunnerHelper runner = setupTrinoWithOpa(new OpaConfig()
                .setAdditionalContextFile(OPA_ADDITIONAL_CONTEXT_FILE)
                .setOpaUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                .setOpaBatchUri(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_BATCH_ALLOW_POLICY_NAME)));

        OPA_CONTAINER.submitPolicy(
                """
                package trino
                import future.keywords.if
                import future.keywords.in
                default allow = false

                allow if is_admin

                allow {
                  is_bob
                  input.action.operation in ["AccessCatalog", "ExecuteQuery", "ImpersonateUser", "ShowSchemas", "SelectFromColumns"]
                }

                batchAllow[i] {
                  some i
                  input.action.filterResources[i]
                  is_admin
                }

                batchAllow[i] {
                  some i
                  is_bob
                  is_some_namespace
                  input.action.operation == "FilterCatalogs"
                  input.action.filterResources[i].catalog.name == "catalog_one"
                }

                is_bob {
                  input.context.identity.user == "bob"
                }

                is_admin {
                  input.context.identity.user == "admin"
                }

                is_some_namespace {
                  input.context.properties.namespace == "some-namespace"
                }
                """);
        Set<String> catalogsForBob = runner.querySetOfStrings("bob", "SHOW CATALOGS");
        assertThat(catalogsForBob).containsExactlyInAnyOrder("catalog_one");
        Set<String> catalogsForAdmin = runner.querySetOfStrings("admin", "SHOW CATALOGS");
        assertThat(catalogsForAdmin).containsExactlyInAnyOrder("catalog_one", "catalog_two", "system");
    }

    private static QueryRunnerHelper setupTrinoWithOpa(OpaConfig opaConfig) {
        QueryRunnerHelper runner = QueryRunnerHelper.withOpaConfig(opaConfig);
        runner.getBaseQueryRunner().installPlugin(new BlackHolePlugin());
        runner.getBaseQueryRunner().createCatalog("catalog_one", "blackhole");
        runner.getBaseQueryRunner().createCatalog("catalog_two", "blackhole");
        return runner;
    }
}
