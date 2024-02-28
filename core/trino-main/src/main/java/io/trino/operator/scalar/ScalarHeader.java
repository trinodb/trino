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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
import static java.util.Objects.requireNonNull;

public class ScalarHeader
{
    private final String name;
    private final Optional<OperatorType> operatorType;
    private final Set<String> aliases;
    private final Optional<String> description;
    private final boolean hidden;
    private final boolean deterministic;

    public ScalarHeader(String name, Set<String> aliases, Optional<String> description, boolean hidden, boolean deterministic)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty());
        this.operatorType = Optional.empty();
        this.aliases = ImmutableSet.copyOf(aliases);
        aliases.forEach(alias -> checkArgument(!alias.isEmpty()));
        this.description = requireNonNull(description, "description is null");
        this.hidden = hidden;
        this.deterministic = deterministic;
    }

    public ScalarHeader(OperatorType operatorType, Optional<String> description)
    {
        this.name = mangleOperatorName(operatorType);
        this.operatorType = Optional.of(operatorType);
        this.description = requireNonNull(description, "description is null");
        this.aliases = ImmutableSet.of();
        this.hidden = true;
        this.deterministic = true;
    }

    public static List<ScalarHeader> fromAnnotatedElement(AnnotatedElement annotated)
    {
        ScalarFunction scalarFunction = annotated.getAnnotation(ScalarFunction.class);
        ScalarOperator scalarOperator = annotated.getAnnotation(ScalarOperator.class);
        Optional<String> description = parseDescription(annotated);

        ImmutableList.Builder<ScalarHeader> builder = ImmutableList.builder();

        if (scalarFunction != null) {
            String baseName = scalarFunction.value().isEmpty() ? camelToSnake(annotatedName(annotated)) : scalarFunction.value();
            builder.add(new ScalarHeader(baseName, ImmutableSet.copyOf(scalarFunction.alias()), description, scalarFunction.hidden(), scalarFunction.deterministic()));
        }

        if (scalarOperator != null) {
            builder.add(new ScalarHeader(scalarOperator.value(), description));
        }

        List<ScalarHeader> result = builder.build();
        checkArgument(!result.isEmpty());
        return result;
    }

    private static String camelToSnake(String name)
    {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    private static String annotatedName(AnnotatedElement annotatedElement)
    {
        if (annotatedElement instanceof Class<?> clazz) {
            return clazz.getSimpleName();
        }
        if (annotatedElement instanceof Method method) {
            return method.getName();
        }

        throw new IllegalArgumentException("Only Classes and Methods are supported as annotated elements.");
    }

    public String getName()
    {
        return name;
    }

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public Set<String> getAliases()
    {
        return aliases;
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
