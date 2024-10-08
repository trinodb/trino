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
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.base.security.TableProcedureAccessControlRule.TableProcedurePrivilege.EXECUTE;
import static java.util.Objects.requireNonNull;

public class TableProcedureAccessControlRule
{
    private final Set<TableProcedurePrivilege> privileges;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> tableProcedureRegex;

    @JsonCreator
    public TableProcedureAccessControlRule(
            @JsonProperty("privileges") Set<TableProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("table_procedure") Optional<Pattern> tableProcedureRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.tableProcedureRegex = requireNonNull(tableProcedureRegex, "tableProcedureRegex is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, String tableProcedure)
    {
        return userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                tableProcedureRegex.map(regex -> regex.matcher(tableProcedure).matches()).orElse(true);
    }

    public boolean canExecuteTableProcedure()
    {
        return privileges.contains(EXECUTE);
    }

    Set<TableProcedurePrivilege> getPrivileges()
    {
        return privileges;
    }

    Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    Optional<Pattern> getRoleRegex()
    {
        return roleRegex;
    }

    Optional<Pattern> getGroupRegex()
    {
        return groupRegex;
    }

    public enum TableProcedurePrivilege
    {
        EXECUTE
    }
}
