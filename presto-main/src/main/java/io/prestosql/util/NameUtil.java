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
package io.prestosql.util;

import io.prestosql.spi.Name;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.Name.createNonDelimitedName;

public class NameUtil
{
    private NameUtil() {}

    public static Name createName(String name)
    {
        SqlParser parser = new SqlParser();
        try {
            Expression expression = parser.createExpression(name, new ParsingOptions());
            checkArgument(expression instanceof Identifier, "Expression of Identifier type is expected");
            return createName((Identifier) expression);
        }
        catch (ParsingException | IllegalArgumentException exception) {
            // Remove this hack once we handle names with special characters on the client side.
            // Mot to handle for quoted identifiers as it would be handled by the SqlParser
            return createNonDelimitedName(name);
        }
    }

    public static Name createName(Identifier identifier)
    {
        return new Name(identifier.getValue(), identifier.isDelimited());
    }
}
