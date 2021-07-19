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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ViewExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.SELECT;
import static java.util.Objects.requireNonNull;

public class TableAccessControlRule
{
    public static final TableAccessControlRule ALLOW_ALL = new TableAccessControlRule(
            ImmutableSet.copyOf(TablePrivilege.values()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final Set<TablePrivilege> privileges;
    private final Map<String, ColumnConstraint> columnConstraints;
    private final Set<String> restrictedColumns;
    private final Optional<String> filter;
    private final Optional<ExpressionEnvironment> filterEnvironment;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> schemaRegex;
    private final Optional<Pattern> tableRegex;

    @JsonCreator
    public TableAccessControlRule(
            @JsonProperty("privileges") Set<TablePrivilege> privileges,
            @JsonProperty("columns") Optional<List<ColumnConstraint>> columns,
            @JsonProperty("filter") Optional<String> filter,
            @JsonProperty("filter_environment") Optional<ExpressionEnvironment> filterEnvironment,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("table") Optional<Pattern> tableRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.columnConstraints = Maps.uniqueIndex(requireNonNull(columns, "columns is null").orElse(ImmutableList.of()), ColumnConstraint::getName);
        this.restrictedColumns = columnConstraints.values().stream()
                .filter(constraint -> !constraint.isAllowed())
                .map(ColumnConstraint::getName)
                .collect(toImmutableSet());
        this.filter = requireNonNull(filter, "filter is null");
        this.filterEnvironment = requireNonNull(filterEnvironment, "filterEnvironment is null");
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "schemaRegex is null");
        this.tableRegex = requireNonNull(tableRegex, "tableRegex is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, SchemaTableName table)
    {
        return userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(table.getSchemaName()).matches()).orElse(true) &&
                tableRegex.map(regex -> regex.matcher(table.getTableName()).matches()).orElse(true);
    }

    public Set<String> getRestrictedColumns()
    {
        return restrictedColumns;
    }

    public boolean canSelectColumns(Set<String> columnNames)
    {
        return (privileges.contains(SELECT) || privileges.contains(GRANT_SELECT)) && restrictedColumns.stream().noneMatch(columnNames::contains);
    }

    public Optional<ViewExpression> getColumnMask(String user, String catalog, String schema, String column)
    {
        return Optional.ofNullable(columnConstraints.get(column)).flatMap(constraint ->
                constraint.getMask().map(mask -> new ViewExpression(
                        constraint.getMaskEnvironment().flatMap(ExpressionEnvironment::getUser).orElse(user),
                        Optional.of(catalog),
                        Optional.of(schema),
                        mask)));
    }

    public Optional<ViewExpression> getFilter(String user, String catalog, String schema)
    {
        return filter.map(filter -> new ViewExpression(
                filterEnvironment.flatMap(ExpressionEnvironment::getUser).orElse(user),
                Optional.of(catalog),
                Optional.of(schema),
                filter));
    }

    Optional<AnySchemaPermissionsRule> toAnySchemaPermissionsRule()
    {
        if (privileges.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnySchemaPermissionsRule(userRegex, roleRegex, groupRegex, schemaRegex));
    }

    Set<TablePrivilege> getPrivileges()
    {
        return privileges;
    }

    Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    public Optional<Pattern> getRoleRegex()
    {
        return roleRegex;
    }

    Optional<Pattern> getGroupRegex()
    {
        return groupRegex;
    }

    Optional<Pattern> getSchemaRegex()
    {
        return schemaRegex;
    }

    public enum TablePrivilege
    {
        SELECT, INSERT, DELETE, UPDATE, OWNERSHIP, GRANT_SELECT
    }
}
