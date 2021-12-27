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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Represents a call over a window:
 * <pre>
 *     classifier OVER (...)
 * </pre>
 * There are two types of window calls supported in Trino:
 * - function calls
 * - row pattern measures
 * This class captures row pattern measures only. A function call over a window
 * is represented as `FunctionCall` having a `Window` member.
 * // TODO refactor `FunctionCall` so that it does not contain `Window`, and instead represent a windowed function call as `WindowOperation`
 */
public class WindowOperation
        extends Expression
{
    private final Identifier name;
    private final Window window;

    public WindowOperation(Identifier name, Window window)
    {
        this(Optional.empty(), name, window);
    }

    public WindowOperation(NodeLocation location, Identifier name, Window window)
    {
        this(Optional.of(location), name, window);
    }

    private WindowOperation(Optional<NodeLocation> location, Identifier name, Window window)
    {
        super(location);
        requireNonNull(name, "name is null");
        requireNonNull(window, "window is null");
        checkArgument(window instanceof WindowReference || window instanceof WindowSpecification, "unexpected window: " + window.getClass().getSimpleName());

        this.name = name;
        this.window = window;
    }

    public Identifier getName()
    {
        return name;
    }

    public Window getWindow()
    {
        return window;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowOperation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(name, (Node) window);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowOperation o = (WindowOperation) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(window, o.window);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, window);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
