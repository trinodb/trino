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

import io.trino.json.ir.TypedValue;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class PathEvaluationContext
{
    // last index of the innermost enclosing array (indexed from 0)
    private final TypedValue last;

    // current item processed by the innermost enclosing filter
    private final Object currentItem;

    public PathEvaluationContext()
    {
        this(new TypedValue(INTEGER, -1), null);
    }

    private PathEvaluationContext(TypedValue last, Object currentItem)
    {
        this.last = last;
        this.currentItem = currentItem;
    }

    public PathEvaluationContext withLast(long last)
    {
        checkArgument(last >= 0, "last array index must not be negative");
        return new PathEvaluationContext(new TypedValue(INTEGER, last), currentItem);
    }

    public PathEvaluationContext withCurrentItem(Object currentItem)
    {
        requireNonNull(currentItem, "currentItem is null");
        return new PathEvaluationContext(last, currentItem);
    }

    public TypedValue getLast()
    {
        if (last.getLongValue() < 0) {
            throw new PathEvaluationError("accessing the last array index with no enclosing array");
        }
        return last;
    }

    public Object getCurrentItem()
    {
        if (currentItem == null) {
            throw new PathEvaluationError("accessing current filter item with no enclosing filter");
        }
        return currentItem;
    }
}
