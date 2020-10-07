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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege;
import io.prestosql.spi.connector.CatalogSchemaTableName;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class CatalogTableAccessControlRule
{
    public static final CatalogTableAccessControlRule ALLOW_ALL = new CatalogTableAccessControlRule(TableAccessControlRule.ALLOW_ALL, Optional.empty());

    private final TableAccessControlRule tableAccessControlRule;
    private final Optional<Pattern> catalogRegex;

    @JsonCreator
    public CatalogTableAccessControlRule(
            @JsonProperty("privileges") Set<TablePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("table") Optional<Pattern> tableRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex)
    {
        this.tableAccessControlRule = new TableAccessControlRule(privileges, userRegex, groupRegex, schemaRegex, tableRegex);
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
    }

    public CatalogTableAccessControlRule(TableAccessControlRule tableAccessControlRule, Optional<Pattern> catalogRegex)
    {
        this.tableAccessControlRule = tableAccessControlRule;
        this.catalogRegex = catalogRegex;
    }

    public Optional<Set<TablePrivilege>> match(String user, Set<String> groups, CatalogSchemaTableName table)
    {
        if (!catalogRegex.map(regex -> regex.matcher(table.getCatalogName()).matches()).orElse(true)) {
            return Optional.empty();
        }
        return tableAccessControlRule.match(user, groups, table.getSchemaTableName());
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (tableAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                tableAccessControlRule.getUserRegex(),
                tableAccessControlRule.getGroupRegex(),
                catalogRegex));
    }

    Optional<AnyCatalogSchemaPermissionsRule> toAnyCatalogSchemaPermissionsRule()
    {
        if (tableAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogSchemaPermissionsRule(
                tableAccessControlRule.getUserRegex(),
                tableAccessControlRule.getGroupRegex(),
                catalogRegex,
                tableAccessControlRule.getSchemaRegex()));
    }
}
