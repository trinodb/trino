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
import io.trino.sql.tree.JsonPathParameter.JsonFormat;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JsonSerialize
        extends Expression
{
    /// Trino extension — SQL:2023 §6.37 JSON_SERIALIZE has no ON ERROR clause. Controls whether malformed input
    /// (parse failure, output-conversion failure) raises a SQL error or yields SQL NULL.
    public enum OnErrorBehavior
    {
        ERROR, NULL
    }

    private final Expression expression;
    private final JsonFormat inputFormat;
    private final Optional<DataType> returnedType;
    private final Optional<JsonFormat> outputFormat;
    private final OnErrorBehavior errorBehavior;

    public JsonSerialize(NodeLocation location, Expression expression, JsonFormat inputFormat, Optional<DataType> returnedType, Optional<JsonFormat> outputFormat, OnErrorBehavior errorBehavior)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.returnedType = requireNonNull(returnedType, "returnedType is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
        this.errorBehavior = requireNonNull(errorBehavior, "errorBehavior is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public JsonFormat getInputFormat()
    {
        return inputFormat;
    }

    public Optional<DataType> getReturnedType()
    {
        return returnedType;
    }

    public Optional<JsonFormat> getOutputFormat()
    {
        return outputFormat;
    }

    public OnErrorBehavior getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonSerialize(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonSerialize that = (JsonSerialize) o;
        return Objects.equals(expression, that.expression) &&
                inputFormat == that.inputFormat &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat) &&
                errorBehavior == that.errorBehavior;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, inputFormat, returnedType, outputFormat, errorBehavior);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonSerialize that = (JsonSerialize) other;
        return inputFormat == that.inputFormat &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat) &&
                errorBehavior == that.errorBehavior;
    }
}
