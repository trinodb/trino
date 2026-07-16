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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class RouterConfig
{
    private Map<String, String> schemaPrefixRules = ImmutableMap.of();
    private Map<String, String> schemaPrefixGlueCatalogIds = ImmutableMap.of();
    private Map<String, List<String>> schemaPrefixMappedSchemas = ImmutableMap.of();
    private Optional<String> defaultCatalogTarget = Optional.empty();
    private Optional<String> defaultCatalogId = Optional.empty();
    private List<String> defaultMappedSchemas = ImmutableList.of();
    private Optional<String> glueRegion = Optional.empty();
    private Optional<String> glueIamRole = Optional.empty();
    private Optional<String> glueExternalId = Optional.empty();
    private Optional<String> glueAwsAccessKey = Optional.empty();
    private Optional<String> glueAwsSecretKey = Optional.empty();
    private boolean glueUseWebIdentityTokenCredentialsProvider;
    private boolean gluePinClientToCurrentRegion;

    public Map<String, String> getSchemaPrefixRules()
    {
        return schemaPrefixRules;
    }

    @Config("router.schema-prefix-rules")
    @ConfigDescription("Comma-separated list of prefix=catalog pairs; schemas matching a prefix are redirected with the prefix stripped (e.g. 'prod_=catalog_prod,staging_=catalog_staging')")
    public RouterConfig setSchemaPrefixRules(List<String> rules)
    {
        this.schemaPrefixRules = parseRules(rules, "router.schema-prefix-rules");
        return this;
    }

    public Map<String, String> getSchemaPrefixGlueCatalogIds()
    {
        return schemaPrefixGlueCatalogIds;
    }

    @Config("router.schema-prefix-glue-catalog-ids")
    @ConfigDescription("Comma-separated list of prefix=glue-catalog-id pairs specifying which Glue catalog (AWS account ID) to query for each prefix; omitting a prefix uses the default account (e.g. 'prod_=111122223333,staging_=444455556666')")
    public RouterConfig setSchemaPrefixGlueCatalogIds(List<String> rules)
    {
        this.schemaPrefixGlueCatalogIds = parseRules(rules, "router.schema-prefix-glue-catalog-ids");
        return this;
    }

    public Map<String, List<String>> getSchemaPrefixMappedSchemas()
    {
        return schemaPrefixMappedSchemas;
    }

    @Config("router.schema-prefix-mapped-schemas")
    @ConfigDescription("Comma-separated list of prefix=regex entries restricting which schemas are exposed per prefix; omitting a prefix allows all schemas (e.g. 'prod_=sales.*,prod_=marketing,staging_=stg_.*')")
    public RouterConfig setSchemaPrefixMappedSchemas(List<String> entries)
    {
        Map<String, ImmutableList.Builder<String>> builders = new LinkedHashMap<>();
        for (String entry : entries) {
            String[] parts = entry.split("=", 2);
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
                throw new IllegalArgumentException("Invalid router.schema-prefix-mapped-schemas entry, expected 'prefix=regex': " + entry);
            }
            try {
                Pattern.compile(parts[1]);
            }
            catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex in router.schema-prefix-mapped-schemas: " + parts[1], e);
            }
            builders.computeIfAbsent(parts[0], _ -> ImmutableList.builder()).add(parts[1]);
        }
        this.schemaPrefixMappedSchemas = builders.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().build()));
        return this;
    }

    public Optional<String> getDefaultCatalogTarget()
    {
        return defaultCatalogTarget;
    }

    @Config("router.default.catalog-target")
    @ConfigDescription("Trino catalog to redirect unprefixed Glue schemas to; enables local Glue schema listing when set")
    public RouterConfig setDefaultCatalogTarget(String defaultCatalogTarget)
    {
        this.defaultCatalogTarget = Optional.ofNullable(defaultCatalogTarget);
        return this;
    }

    public Optional<String> getDefaultCatalogId()
    {
        return defaultCatalogId;
    }

    @Config("router.default.catalog-id")
    @ConfigDescription("Glue catalog ID (AWS account ID) for unprefixed schema listing; omit to use the default account")
    public RouterConfig setDefaultCatalogId(String defaultCatalogId)
    {
        this.defaultCatalogId = Optional.ofNullable(defaultCatalogId);
        return this;
    }

    public List<String> getDefaultMappedSchemas()
    {
        return defaultMappedSchemas;
    }

    @Config("router.default.mapped-schemas")
    @ConfigDescription("Comma-separated list of Java regexes restricting which unprefixed schemas are exposed; empty allows all (e.g. 'sales.*,marketing')")
    public RouterConfig setDefaultMappedSchemas(List<String> patterns)
    {
        for (String pattern : patterns) {
            try {
                Pattern.compile(pattern);
            }
            catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex in router.default.mapped-schemas: " + pattern, e);
            }
        }
        this.defaultMappedSchemas = ImmutableList.copyOf(patterns);
        return this;
    }

    public Optional<String> getGlueRegion()
    {
        return glueRegion;
    }

    @Config("router.glue.region")
    @ConfigDescription("AWS region for the Glue Data Catalog used to enumerate prefix-mapped schemas")
    public RouterConfig setGlueRegion(String glueRegion)
    {
        this.glueRegion = Optional.ofNullable(glueRegion);
        return this;
    }

    public Optional<String> getGlueIamRole()
    {
        return glueIamRole;
    }

    @Config("router.glue.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to Glue")
    public RouterConfig setGlueIamRole(String glueIamRole)
    {
        this.glueIamRole = Optional.ofNullable(glueIamRole);
        return this;
    }

    public Optional<String> getGlueExternalId()
    {
        return glueExternalId;
    }

    @Config("router.glue.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to Glue")
    public RouterConfig setGlueExternalId(String glueExternalId)
    {
        this.glueExternalId = Optional.ofNullable(glueExternalId);
        return this;
    }

    public Optional<String> getGlueAwsAccessKey()
    {
        return glueAwsAccessKey;
    }

    @Config("router.glue.aws-access-key")
    @ConfigDescription("AWS access key for Glue")
    public RouterConfig setGlueAwsAccessKey(String glueAwsAccessKey)
    {
        this.glueAwsAccessKey = Optional.ofNullable(glueAwsAccessKey);
        return this;
    }

    public Optional<String> getGlueAwsSecretKey()
    {
        return glueAwsSecretKey;
    }

    @Config("router.glue.aws-secret-key")
    @ConfigDescription("AWS secret key for Glue")
    @ConfigSecuritySensitive
    public RouterConfig setGlueAwsSecretKey(String glueAwsSecretKey)
    {
        this.glueAwsSecretKey = Optional.ofNullable(glueAwsSecretKey);
        return this;
    }

    public boolean isGlueUseWebIdentityTokenCredentialsProvider()
    {
        return glueUseWebIdentityTokenCredentialsProvider;
    }

    @Config("router.glue.use-web-identity-token-credentials-provider")
    @ConfigDescription("Use the WebIdentityTokenCredentialsProvider for Glue authentication")
    public RouterConfig setGlueUseWebIdentityTokenCredentialsProvider(boolean glueUseWebIdentityTokenCredentialsProvider)
    {
        this.glueUseWebIdentityTokenCredentialsProvider = glueUseWebIdentityTokenCredentialsProvider;
        return this;
    }

    public boolean isGluePinClientToCurrentRegion()
    {
        return gluePinClientToCurrentRegion;
    }

    @Config("router.glue.pin-client-to-current-region")
    @ConfigDescription("Pin the Glue client to the current EC2 region")
    public RouterConfig setGluePinClientToCurrentRegion(boolean gluePinClientToCurrentRegion)
    {
        this.gluePinClientToCurrentRegion = gluePinClientToCurrentRegion;
        return this;
    }

    private static Map<String, String> parseRules(List<String> rules, String configName)
    {
        return rules.stream()
                .map(rule -> rule.split("=", 2))
                .peek(parts -> {
                    if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
                        throw new IllegalArgumentException("Invalid " + configName + " entry, expected 'key=catalog': " + String.join("=", parts));
                    }
                })
                .collect(toImmutableMap(parts -> parts[0], parts -> parts[1]));
    }
}
