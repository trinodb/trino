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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.ViewExpression;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class CatalogTableAccessControlRule
{
    public static final CatalogTableAccessControlRule ALLOW_ALL = new CatalogTableAccessControlRule(TableAccessControlRule.ALLOW_ALL, Optional.empty());

    private final TableAccessControlRule tableAccessControlRule;
    private final Optional<UserSubstitutingPattern> catalogPattern;

    @JsonCreator
    public CatalogTableAccessControlRule(
            @JsonProperty("privileges") Set<TablePrivilege> privileges,
            @JsonProperty("columns") Optional<List<ColumnConstraint>> columns,
            @JsonProperty("filter") Optional<String> filter,
            @JsonProperty("filter_environment") Optional<ExpressionEnvironment> filterEnvironment,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") @JsonDeserialize(contentUsing = UserSubstitutingPattern.Deserializer.class) Optional<UserSubstitutingPattern> schemaPattern,
            @JsonProperty("table") @JsonDeserialize(contentUsing = UserSubstitutingPattern.Deserializer.class) Optional<UserSubstitutingPattern> tablePattern,
            @JsonProperty("catalog") @JsonDeserialize(contentUsing = UserSubstitutingPattern.Deserializer.class) Optional<UserSubstitutingPattern> catalogPattern)
    {
        this.tableAccessControlRule = new TableAccessControlRule(privileges, columns, filter, filterEnvironment, userRegex, roleRegex, groupRegex, schemaPattern, tablePattern);
        this.catalogPattern = requireNonNull(catalogPattern, "catalogPattern is null");
    }

    public CatalogTableAccessControlRule(TableAccessControlRule tableAccessControlRule, Optional<UserSubstitutingPattern> catalogPattern)
    {
        this.tableAccessControlRule = tableAccessControlRule;
        this.catalogPattern = catalogPattern;
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, CatalogSchemaTableName table)
    {
        if (!catalogPattern.map(p -> p.matches(user, table.getCatalogName())).orElse(true)) {
            return false;
        }
        return tableAccessControlRule.matches(user, roles, groups, table.getSchemaTableName());
    }

    public Set<TablePrivilege> getPrivileges()
    {
        return tableAccessControlRule.getPrivileges();
    }

    public Set<String> getRestrictedColumns()
    {
        return tableAccessControlRule.getRestrictedColumns();
    }

    public boolean canSelectColumns(Set<String> columnNames)
    {
        return tableAccessControlRule.canSelectColumns(columnNames);
    }

    public Optional<ViewExpression> getColumnMask(String catalog, String schema, String column)
    {
        return tableAccessControlRule.getColumnMask(catalog, schema, column);
    }

    public Optional<ViewExpression> getFilter(String catalog, String schema)
    {
        return tableAccessControlRule.getFilter(catalog, schema);
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (tableAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                tableAccessControlRule.getUserRegex(),
                tableAccessControlRule.getRoleRegex(),
                tableAccessControlRule.getGroupRegex(),
                catalogPattern));
    }

    Optional<AnyCatalogSchemaPermissionsRule> toAnyCatalogSchemaPermissionsRule()
    {
        if (tableAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogSchemaPermissionsRule(
                tableAccessControlRule.getUserRegex(),
                tableAccessControlRule.getRoleRegex(),
                tableAccessControlRule.getGroupRegex(),
                catalogPattern,
                tableAccessControlRule.getSchemaPattern()));
    }
}
