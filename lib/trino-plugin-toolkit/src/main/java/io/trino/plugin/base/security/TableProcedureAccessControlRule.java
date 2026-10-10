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
    private final IdentityMatcher identityMatcher;
    private final Optional<Pattern> procedureRegex;

    @JsonCreator
    public TableProcedureAccessControlRule(
            @JsonProperty("privileges") Set<TableProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("procedure") Optional<Pattern> procedureRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.identityMatcher = new IdentityMatcher(userRegex, roleRegex, groupRegex);
        this.procedureRegex = requireNonNull(procedureRegex, "procedureRegex is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, String procedureName)
    {
        return identityMatcher.matches(user, roles, groups) &&
                procedureRegex.map(regex -> regex.matcher(procedureName).matches()).orElse(true);
    }

    public boolean canExecuteTableProcedure()
    {
        return privileges.contains(EXECUTE);
    }

    Set<TableProcedurePrivilege> getPrivileges()
    {
        return privileges;
    }

    IdentityMatcher getIdentityMatcher()
    {
        return identityMatcher;
    }

    public enum TableProcedurePrivilege
    {
        EXECUTE
    }
}
