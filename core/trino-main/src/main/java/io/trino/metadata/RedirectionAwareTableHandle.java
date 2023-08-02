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
package io.trino.metadata;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record RedirectionAwareTableHandle(
        Optional<TableHandle> tableHandle,
        // the target table name after redirection. Optional.empty() if the table is not redirected.
        Optional<QualifiedObjectName> redirectedTableName)
{
    public static RedirectionAwareTableHandle withRedirectionTo(QualifiedObjectName redirectedTableName, TableHandle tableHandle)
    {
        return new RedirectionAwareTableHandle(Optional.of(tableHandle), Optional.of(redirectedTableName));
    }

    public static RedirectionAwareTableHandle noRedirection(Optional<TableHandle> tableHandle)
    {
        return new RedirectionAwareTableHandle(tableHandle, Optional.empty());
    }

    public RedirectionAwareTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(redirectedTableName, "redirectedTableName is null");
        // Table handle must exist if there is redirection
        checkArgument(tableHandle.isPresent() || redirectedTableName.isEmpty(), "redirectedTableName present without tableHandle");
    }
}
