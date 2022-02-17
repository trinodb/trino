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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.util.Objects.requireNonNull;

public class RewriteFunctionCall
        implements ConnectorExpressionRule<Call, String>
{
    private static final List<Capture<ConnectorExpression>> ARGUMENTS = ImmutableList.of(
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture(),
            newCapture());

    private final String functionName;
    private final Function<List<String>, String> functionRewrite;
    private final Class<? extends Type> resultType;
    private final List<Class<? extends Type>> argumentTypes;

    private RewriteFunctionCall(String functionName, Function<List<String>, String> functionRewrite, Class<? extends Type> resultType, List<Class<? extends Type>> argumentTypes)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.functionRewrite = requireNonNull(functionRewrite, "functionRewrite is null");
        this.resultType = requireNonNull(resultType, "resultType is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");

        verify(argumentTypes.size() < ARGUMENTS.size(), "Could not capture function call with arguments list bigger than %s", ARGUMENTS.size());
    }

    @Override
    public Pattern<Call> getPattern()
    {
        Pattern<Call> pattern = call()
                .with(functionName().equalTo(functionName))
                .with(type().matching(resultType::isInstance))
                .with(argumentCount().equalTo(argumentTypes.size()));

        for (int i = 0; i < argumentTypes.size(); i++) {
            pattern = pattern.with(argument(i).matching(expression().capturedAs(ARGUMENTS.get(i)).with(type().matching(argumentTypes.get(i)::isInstance))));
        }

        return pattern;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        ImmutableList.Builder<String> arguments = ImmutableList.builderWithExpectedSize(argumentTypes.size());

        for (int i = 0; i < argumentTypes.size(); i++) {
            Optional<String> argument = context.defaultRewrite(captures.get(ARGUMENTS.get(i)));
            if (argument.isEmpty()) {
                return Optional.empty();
            }

            arguments.add(argument.get());
        }

        return Optional.of(functionRewrite.apply(arguments.build()));
    }

    public static Builder builder(String name)
    {
        return new Builder(name);
    }

    static class Builder
    {
        private String name;
        private Function<List<String>, String> function;
        private Class<? extends Type> resultType;
        private List<Class<? extends Type>> argumentTypes;

        Builder(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public Builder withRewrite(Function<List<String>, String> function)
        {
            this.function = requireNonNull(function, "function is null");
            return this;
        }

        public Builder withResultType(Class<? extends Type> resultType)
        {
            this.resultType = requireNonNull(resultType, "resultType is null");
            return this;
        }

        @SafeVarargs
        public final Builder withArgumentTypes(Class<? extends Type>... arguments)
        {
            this.argumentTypes = ImmutableList.copyOf(arguments);
            return this;
        }

        public RewriteFunctionCall build()
        {
            return new RewriteFunctionCall(name, function, resultType, argumentTypes);
        }
    }
}
