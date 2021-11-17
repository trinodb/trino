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
package io.trino.plugin.jdbc;

import static java.util.Objects.requireNonNull;

public final class RemoteJdbcColumnHandle
{
    private final JdbcColumnHandle columnHandle;
    private final String remoteName;

    /**
     * JdbcColumnHandle stores the mapped name based on IdentifierMapping
     * RemoteJdbcColumnHandle simply wraps the JdbcColumnHandle with extra information about the remoteName used by the underlying JDBC db
     * @param columnHandle JdbcColumnHandle
     * @param remoteName The remoteName of the column in the underlying database
     */
    public RemoteJdbcColumnHandle(JdbcColumnHandle columnHandle, String remoteName)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        this.remoteName = requireNonNull(remoteName, "remoteName is null");
    }

    public JdbcColumnHandle getColumnHandle()
    {
        return columnHandle;
    }

    public String getRemoteName()
    {
        return remoteName;
    }
}
