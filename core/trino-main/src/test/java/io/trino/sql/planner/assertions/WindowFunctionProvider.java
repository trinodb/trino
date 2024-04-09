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
package io.trino.sql.planner.assertions;

import com.google.common.base.Joiner;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;

import static io.trino.sql.planner.assertions.PlanMatchPattern.toSymbolReferences;
import static java.util.Objects.requireNonNull;

final class WindowFunctionProvider
        implements ExpectedValueProvider<WindowFunction>
{
    private final String name;
    private final WindowNode.Frame frame;
    private final List<PlanTestSymbol> args;

    public WindowFunctionProvider(String name, WindowNode.Frame frame, List<PlanTestSymbol> args)
    {
        this.name = requireNonNull(name, "name is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.args = requireNonNull(args, "args is null");
    }

    @Override
    public String toString()
    {
        return "%s(%s) %s".formatted(
                name,
                Joiner.on(", ").join(args),
                frame);
    }

    @Override
    public WindowFunction getExpectedValue(SymbolAliases aliases)
    {
        return new WindowFunction(name, frame, toSymbolReferences(args, aliases));
    }
}
