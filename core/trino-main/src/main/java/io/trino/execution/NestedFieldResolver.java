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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Node;

import java.util.List;

import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

final class NestedFieldResolver
{
    private NestedFieldResolver() {}

    record ResolvedField(List<String> path, Type type) {}

    /**
     * Resolves a field path within a column type, traversing only ROW types.
     */
    static ResolvedField resolve(Node node, String columnName, Type columnType, List<String> fieldPath)
    {
        ImmutableList.Builder<String> resolvedPath = ImmutableList.builder();
        Type currentType = columnType;
        for (String fieldName : fieldPath) {
            if (!(currentType instanceof RowType rowType)) {
                throw semanticException(NOT_SUPPORTED, node, "Cannot resolve field '%s' within %s type; only ROW fields are supported", fieldName, currentType);
            }
            List<RowType.Field> candidates = rowType.getFields().stream()
                    .filter(field -> field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName))
                    .toList();
            if (candidates.isEmpty()) {
                throw semanticException(COLUMN_NOT_FOUND, node, "Field '%s' does not exist in column '%s'", fieldName, columnName);
            }
            if (candidates.size() > 1) {
                throw semanticException(COLUMN_NOT_FOUND, node, "Field '%s' in column '%s' is ambiguous", fieldName, columnName);
            }
            RowType.Field field = candidates.getFirst();
            resolvedPath.add(field.getName().get());
            currentType = field.getType();
        }
        return new ResolvedField(resolvedPath.build(), currentType);
    }
}
