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
package io.trino.execution;

import io.trino.metadata.Metadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Node;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public final class PrivilegeUtilities
{
    private PrivilegeUtilities() {}

    public static Set<Privilege> parseStatementPrivileges(Node statement, Optional<List<String>> optionalPrivileges)
    {
        Set<Privilege> privileges;
        if (optionalPrivileges.isPresent()) {
            privileges = optionalPrivileges.get().stream()
                    .map(privilege -> parsePrivilege(statement, privilege))
                    .collect(toImmutableSet());
        }
        else {
            // All privileges
            privileges = EnumSet.allOf(Privilege.class);
        }
        return privileges;
    }

    public static Set<EntityPrivilege> fetchEntityKindPrivileges(String entityKind, Metadata metadata, Optional<List<String>> privileges)
    {
        Set<EntityPrivilege> allPrivileges = metadata.getAllEntityKindPrivileges(entityKind);
        if (privileges.isPresent()) {
            return privileges.get().stream()
                    .map(privilege -> {
                        EntityPrivilege entityPrivilege = new EntityPrivilege(privilege.toUpperCase(Locale.ENGLISH));
                        if (!allPrivileges.contains(entityPrivilege)) {
                            throw new TrinoException(INVALID_PRIVILEGE, "Privilege %s is not supported for entity kind %s".formatted(privilege, entityKind));
                        }
                        return entityPrivilege;
                    }).collect(toImmutableSet());
        }
        else {
            return allPrivileges;
        }
    }

    private static Privilege parsePrivilege(Node statement, String privilegeString)
    {
        for (Privilege privilege : Privilege.values()) {
            if (privilege.name().equalsIgnoreCase(privilegeString)) {
                return privilege;
            }
        }

        throw semanticException(INVALID_PRIVILEGE, statement, "Unknown privilege: '%s'", privilegeString);
    }
}
