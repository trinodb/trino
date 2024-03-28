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
package io.trino.spi.connector;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.UNSUPPORTED_VIEW_DIALECT;
import static java.util.Objects.requireNonNull;

public class UnsupportedViewDialectException
        extends TrinoException
{
    private final SchemaTableName viewName;
    private final String dialect;

    public UnsupportedViewDialectException(SchemaTableName viewName, String dialect)
    {
        super(UNSUPPORTED_VIEW_DIALECT, String.format("View %s has dialect %s which is not supported", viewName, dialect));
        this.viewName = requireNonNull(viewName, "viewName is null");
        this.dialect = requireNonNull(dialect, "dialect is null");
    }

    public SchemaTableName getViewName()
    {
        return viewName;
    }

    public String getDialect()
    {
        return dialect;
    }
}
