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

import static io.trino.sql.ExpressionFormatter.formatJsonPathInvocation;
import static java.util.Objects.requireNonNull;

public class JsonPathInvocation
        extends Node
{
    private final Expression inputExpression;
    private final JsonFormat inputFormat;
    private final StringLiteral jsonPath;
    private final Optional<Identifier> pathName;
    private final List<JsonPathParameter> pathParameters;

    public JsonPathInvocation(
            Optional<NodeLocation> location,
            Expression inputExpression,
            JsonFormat inputFormat,
            StringLiteral jsonPath,
            Optional<Identifier> pathName,
            List<JsonPathParameter> pathParameters)
    {
        super(location);
        requireNonNull(inputExpression, "inputExpression is null");
        requireNonNull(inputFormat, "inputFormat is null");
        requireNonNull(jsonPath, "jsonPath is null");
        requireNonNull(pathName, "pathName is null");
        requireNonNull(pathParameters, "pathParameters is null");

        this.inputExpression = inputExpression;
        this.inputFormat = inputFormat;
        this.jsonPath = jsonPath;
        this.pathName = pathName;
        this.pathParameters = ImmutableList.copyOf(pathParameters);
    }

    public Expression getInputExpression()
    {
        return inputExpression;
    }

    public JsonFormat getInputFormat()
    {
        return inputFormat;
    }

    public StringLiteral getJsonPath()
    {
        return jsonPath;
    }

    public Optional<Identifier> getPathName()
    {
        return pathName;
    }

    public List<JsonPathParameter> getPathParameters()
    {
        return pathParameters;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonPathInvocation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(inputExpression);
        children.add(jsonPath);
        pathParameters
                .forEach(children::add);
        return children.build();
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

        JsonPathInvocation that = (JsonPathInvocation) o;
        return Objects.equals(inputExpression, that.inputExpression) &&
                inputFormat == that.inputFormat &&
                Objects.equals(jsonPath, that.jsonPath) &&
                Objects.equals(pathName, that.pathName) &&
                Objects.equals(pathParameters, that.pathParameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(inputExpression, inputFormat, jsonPath, pathName, pathParameters);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonPathInvocation otherInvocation = (JsonPathInvocation) other;
        return inputFormat == otherInvocation.inputFormat &&
                pathName.equals(otherInvocation.getPathName());
    }

    @Override
    public String toString()
    {
        return formatJsonPathInvocation(this);
    }
}
