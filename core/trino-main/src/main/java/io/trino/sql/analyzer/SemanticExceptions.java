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
package io.trino.sql.analyzer;

import com.google.errorprone.annotations.FormatMethod;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static java.lang.String.format;

public final class SemanticExceptions
{
    private SemanticExceptions() {}

    public static TrinoException missingAttributeException(Expression node, QualifiedName name)
    {
        throw semanticException(COLUMN_NOT_FOUND, node, "Column '%s' cannot be resolved", name);
    }

    public static TrinoException invalidReferenceException(Expression node, QualifiedName name)
    {
        throw semanticException(INVALID_COLUMN_REFERENCE, node, "Column reference '%s' is invalid", name);
    }

    public static TrinoException ambiguousAttributeException(Expression node, QualifiedName name)
    {
        throw semanticException(AMBIGUOUS_NAME, node, "Column '%s' is ambiguous", name);
    }

    @SuppressWarnings("FormatStringAnnotation")
    @FormatMethod
    public static TrinoException semanticException(ErrorCodeSupplier code, Node node, String format, Object... args)
    {
        return semanticException(code, node, null, format, args);
    }

    @FormatMethod
    public static TrinoException semanticException(ErrorCodeSupplier code, Node node, Throwable cause, String format, Object... args)
    {
        throw new TrinoException(code, extractLocation(node), format(format, args), cause);
    }
}
