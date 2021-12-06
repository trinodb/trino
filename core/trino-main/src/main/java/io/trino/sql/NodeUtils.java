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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.SortItem;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public final class NodeUtils
{
    private NodeUtils() {}

    public static List<SortItem> getSortItemsFromOrderBy(Optional<OrderBy> orderBy)
    {
        return orderBy.map(OrderBy::getSortItems).orElse(ImmutableList.of());
    }

    /**
     * Throws an exception if {@code properties} contains a {@code Property} set to DEFAULT. This method is intended to be used when a user
     * issues an SQL statement containing a property set to DEFAULT but setting a property to DEFAULT is not supported in that type of
     * statement.
     * <p>
     * {@code statementType} is a {@code String} indicating the type of the statement, e.g. "CREATE TABLE", "ANALYZE",
     * "ALTER TABLE ... ADD COLUMN ...".
     */
    public static void throwOnDefaultProperty(Collection<Property> properties, String statementType)
    {
        requireNonNull(properties, "properties is null");
        requireNonNull(statementType, "statementType is null");
        properties.stream()
                .filter(Property::isSetToDefault)
                .findFirst()
                .ifPresent(defaultProperty -> {
                    throw semanticException(NOT_SUPPORTED, defaultProperty, "Setting a property to DEFAULT is not supported in %s", statementType);
                });
    }
}
