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
package io.trino.plugin.router;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRouterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RouterConfig.class)
                .setSchemaPrefixRules(List.of())
                .setSchemaPrefixGlueCatalogIds(List.of())
                .setSchemaPrefixMappedSchemas(List.of())
                .setDefaultCatalogTarget(null)
                .setDefaultCatalogId(null)
                .setDefaultMappedSchemas(List.of())
                .setGlueRegion(null)
                .setGlueIamRole(null)
                .setGlueExternalId(null)
                .setGlueAwsAccessKey(null)
                .setGlueAwsSecretKey(null)
                .setGlueUseWebIdentityTokenCredentialsProvider(false)
                .setGluePinClientToCurrentRegion(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("router.schema-prefix-rules", "prod_=catalog_prod,staging_=catalog_staging")
                .put("router.schema-prefix-glue-catalog-ids", "prod_=111122223333,staging_=444455556666")
                .put("router.schema-prefix-mapped-schemas", "prod_=sales.*,prod_=marketing,staging_=stg_.*")
                .put("router.default.catalog-target", "catalog_default")
                .put("router.default.catalog-id", "999900001111")
                .put("router.default.mapped-schemas", "sales.*,marketing")
                .put("router.glue.region", "us-east-1")
                .put("router.glue.iam-role", "arn:aws:iam::123456789012:role/GlueRole")
                .put("router.glue.external-id", "my-external-id")
                .put("router.glue.aws-access-key", "AKIAIOSFODNN7EXAMPLE")
                .put("router.glue.aws-secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .put("router.glue.use-web-identity-token-credentials-provider", "true")
                .put("router.glue.pin-client-to-current-region", "true")
                .buildOrThrow();

        RouterConfig expected = new RouterConfig()
                .setSchemaPrefixRules(List.of("prod_=catalog_prod", "staging_=catalog_staging"))
                .setSchemaPrefixGlueCatalogIds(List.of("prod_=111122223333", "staging_=444455556666"))
                .setSchemaPrefixMappedSchemas(List.of("prod_=sales.*", "prod_=marketing", "staging_=stg_.*"))
                .setDefaultCatalogTarget("catalog_default")
                .setDefaultCatalogId("999900001111")
                .setDefaultMappedSchemas(List.of("sales.*", "marketing"))
                .setGlueRegion("us-east-1")
                .setGlueIamRole("arn:aws:iam::123456789012:role/GlueRole")
                .setGlueExternalId("my-external-id")
                .setGlueAwsAccessKey("AKIAIOSFODNN7EXAMPLE")
                .setGlueAwsSecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .setGlueUseWebIdentityTokenCredentialsProvider(true)
                .setGluePinClientToCurrentRegion(true);

        assertFullMapping(properties, expected);
    }
}
