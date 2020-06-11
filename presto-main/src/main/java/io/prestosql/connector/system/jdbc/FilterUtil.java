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
package io.prestosql.connector.system.jdbc;

import io.prestosql.metadata.QualifiedTablePrefix;

import java.util.Optional;

public final class FilterUtil
{
    private FilterUtil() {}

    public static QualifiedTablePrefix tablePrefix(String catalog, Optional<String> schema, Optional<String> table)
    {
        if (schema.isPresent() && table.isPresent()) {
            return new QualifiedTablePrefix(catalog, schema.get(), table.get());
        }
        if (schema.isPresent()) {
            return new QualifiedTablePrefix(catalog, schema.get());
        }
        return new QualifiedTablePrefix(catalog);
    }
}
