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
package io.trino.json;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.PATH_EVALUATION_ERROR;
import static java.lang.String.format;

public class PathEvaluationError
        extends TrinoException
{
    public PathEvaluationError(String message)
    {
        super(PATH_EVALUATION_ERROR, "path evaluation failed: " + message);
    }

    public PathEvaluationError(Throwable cause)
    {
        super(PATH_EVALUATION_ERROR, "path evaluation failed: ", cause);
    }

    /**
     * An exception resulting from a structural error during JSON path evaluation.
     * <p>
     * A structural error occurs when the JSON path expression attempts to access a
     * non-existent element of a JSON array or a non-existent member of a JSON object.
     * <p>
     * Note: in `lax` mode, the structural errors are suppressed, and the erroneous
     * subexpression is evaluated to an empty sequence. In `strict` mode, the structural
     * errors are propagated to the enclosing function (i.e. the function within which
     * the path is evaluated, e.g. `JSON_EXISTS`), and there they are handled accordingly
     * to the chosen `ON ERROR` option. Non-structural errors (e.g. numeric exceptions)
     * are not suppressed in `lax` or `strict` mode.
     */
    public static TrinoException structuralError(String format, Object... arguments)
    {
        return new PathEvaluationError("structural error: " + format(format, arguments));
    }

    public static TrinoException itemTypeError(String expected, String actual)
    {
        return new PathEvaluationError(format("invalid item type. Expected: %s, actual: %s", expected, actual));
    }
}
