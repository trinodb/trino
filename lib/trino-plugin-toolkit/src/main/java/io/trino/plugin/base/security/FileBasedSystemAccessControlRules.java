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
package io.trino.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class FileBasedSystemAccessControlRules
{
    private final Optional<List<CatalogAccessControlRule>> catalogRules;
    private final Optional<List<QueryAccessRule>> queryAccessRules;
    private final Optional<List<ImpersonationRule>> impersonationRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final Optional<List<SystemInformationRule>> systemInformationRules;
    private final Optional<List<CatalogSchemaAccessControlRule>> schemaRules;
    private final Optional<List<CatalogTableAccessControlRule>> tableRules;
    private final Optional<List<SessionPropertyAccessControlRule>> sessionPropertyRules;
    private final Optional<List<CatalogSessionPropertyAccessControlRule>> catalogSessionPropertyRules;

    @JsonCreator
    public FileBasedSystemAccessControlRules(
            @JsonProperty("catalogs") Optional<List<CatalogAccessControlRule>> catalogRules,
            @JsonProperty("queries") Optional<List<QueryAccessRule>> queryAccessRules,
            @JsonProperty("impersonation") Optional<List<ImpersonationRule>> impersonationRules,
            @JsonProperty("principals") Optional<List<PrincipalUserMatchRule>> principalUserMatchRules,
            @JsonProperty("system_information") Optional<List<SystemInformationRule>> systemInformationRules,
            @JsonProperty("schemas") Optional<List<CatalogSchemaAccessControlRule>> schemaAccessControlRules,
            @JsonProperty("tables") Optional<List<CatalogTableAccessControlRule>> tableAccessControlRules,
            @JsonProperty("system_session_properties") Optional<List<SessionPropertyAccessControlRule>> sessionPropertyRules,
            @JsonProperty("catalog_session_properties") Optional<List<CatalogSessionPropertyAccessControlRule>> catalogSessionPropertyRules)
    {
        this.catalogRules = catalogRules.map(ImmutableList::copyOf);
        this.queryAccessRules = queryAccessRules.map(ImmutableList::copyOf);
        this.principalUserMatchRules = principalUserMatchRules.map(ImmutableList::copyOf);
        this.impersonationRules = impersonationRules.map(ImmutableList::copyOf);
        this.systemInformationRules = systemInformationRules.map(ImmutableList::copyOf);
        this.schemaRules = schemaAccessControlRules.map(ImmutableList::copyOf);
        this.tableRules = tableAccessControlRules.map(ImmutableList::copyOf);
        this.sessionPropertyRules = sessionPropertyRules.map(ImmutableList::copyOf);
        this.catalogSessionPropertyRules = catalogSessionPropertyRules.map(ImmutableList::copyOf);
    }

    public Optional<List<CatalogAccessControlRule>> getCatalogRules()
    {
        return catalogRules;
    }

    public Optional<List<QueryAccessRule>> getQueryAccessRules()
    {
        return queryAccessRules;
    }

    public Optional<List<ImpersonationRule>> getImpersonationRules()
    {
        return impersonationRules;
    }

    public Optional<List<PrincipalUserMatchRule>> getPrincipalUserMatchRules()
    {
        return principalUserMatchRules;
    }

    public Optional<List<SystemInformationRule>> getSystemInformationRules()
    {
        return systemInformationRules;
    }

    public Optional<List<CatalogSchemaAccessControlRule>> getSchemaRules()
    {
        return schemaRules;
    }

    public Optional<List<CatalogTableAccessControlRule>> getTableRules()
    {
        return tableRules;
    }

    public Optional<List<SessionPropertyAccessControlRule>> getSessionPropertyRules()
    {
        return sessionPropertyRules;
    }

    public Optional<List<CatalogSessionPropertyAccessControlRule>> getCatalogSessionPropertyRules()
    {
        return catalogSessionPropertyRules;
    }
}
