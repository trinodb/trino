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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;

public final class SessionFunctions
{
    private SessionFunctions() {}

    @ScalarFunction(value = "$current_user", hidden = true)
    @Description("Current user")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentUser(ConnectorSession session)
    {
        return utf8Slice(session.getUser());
    }

    @ScalarFunction(value = "$current_path", hidden = true)
    @Description("Retrieve current path")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentPath(ConnectorSession session)
    {
        // this function is a language construct and has special access to internals
        return utf8Slice(((FullConnectorSession) session).getSession().getPath().toString());
    }

    @ScalarFunction(value = "$current_catalog", hidden = true)
    @Description("Current catalog")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentCatalog(ConnectorSession session)
    {
        return ((FullConnectorSession) session).getSession().getCatalog()
                .map(Slices::utf8Slice)
                .orElse(null);
    }

    @ScalarFunction(value = "$current_schema", hidden = true)
    @Description("Current schema")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentSchema(ConnectorSession session)
    {
        return ((FullConnectorSession) session).getSession().getSchema()
                .map(Slices::utf8Slice)
                .orElse(null);
    }
}
