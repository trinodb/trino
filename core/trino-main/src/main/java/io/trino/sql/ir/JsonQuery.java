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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.JsonPathParameter.JsonFormat;
import io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior;
import io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior;
import io.trino.sql.tree.JsonQuery.QuotesBehavior;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class JsonQuery
        extends Expression
{
    private final JsonPathInvocation jsonPathInvocation;
    private final Optional<DataType> returnedType;
    private final Optional<JsonFormat> outputFormat;
    private final ArrayWrapperBehavior wrapperBehavior;
    private final Optional<QuotesBehavior> quotesBehavior;
    private final EmptyOrErrorBehavior emptyBehavior;
    private final EmptyOrErrorBehavior errorBehavior;

    @JsonCreator
    public JsonQuery(
            @JsonProperty("jsonPathInvocation") JsonPathInvocation jsonPathInvocation,
            @JsonProperty("returnedType") Optional<DataType> returnedType,
            @JsonProperty("outputFormat") Optional<JsonFormat> outputFormat,
            @JsonProperty("wrapperBehavior") ArrayWrapperBehavior wrapperBehavior,
            @JsonProperty("quotesBehavior") Optional<QuotesBehavior> quotesBehavior,
            @JsonProperty("emptyBehavior") EmptyOrErrorBehavior emptyBehavior,
            @JsonProperty("errorBehavior") EmptyOrErrorBehavior errorBehavior)
    {
        requireNonNull(jsonPathInvocation, "jsonPathInvocation is null");
        requireNonNull(returnedType, "returnedType is null");
        requireNonNull(outputFormat, "outputFormat is null");
        requireNonNull(wrapperBehavior, "wrapperBehavior is null");
        requireNonNull(quotesBehavior, "quotesBehavior is null");
        requireNonNull(emptyBehavior, "emptyBehavior is null");
        requireNonNull(errorBehavior, "errorBehavior is null");

        this.jsonPathInvocation = jsonPathInvocation;
        this.returnedType = returnedType;
        this.outputFormat = outputFormat;
        this.wrapperBehavior = wrapperBehavior;
        this.quotesBehavior = quotesBehavior;
        this.emptyBehavior = emptyBehavior;
        this.errorBehavior = errorBehavior;
    }

    @JsonProperty
    public JsonPathInvocation getJsonPathInvocation()
    {
        return jsonPathInvocation;
    }

    @JsonProperty
    public Optional<DataType> getReturnedType()
    {
        return returnedType;
    }

    @JsonProperty
    public Optional<JsonFormat> getOutputFormat()
    {
        return outputFormat;
    }

    @JsonProperty
    public ArrayWrapperBehavior getWrapperBehavior()
    {
        return wrapperBehavior;
    }

    @JsonProperty
    public Optional<QuotesBehavior> getQuotesBehavior()
    {
        return quotesBehavior;
    }

    @JsonProperty
    public EmptyOrErrorBehavior getEmptyBehavior()
    {
        return emptyBehavior;
    }

    @JsonProperty
    public EmptyOrErrorBehavior getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonQuery(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(jsonPathInvocation);
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

        JsonQuery that = (JsonQuery) o;
        return Objects.equals(jsonPathInvocation, that.jsonPathInvocation) &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat) &&
                wrapperBehavior == that.wrapperBehavior &&
                Objects.equals(quotesBehavior, that.quotesBehavior) &&
                emptyBehavior == that.emptyBehavior &&
                errorBehavior == that.errorBehavior;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPathInvocation, returnedType, outputFormat, wrapperBehavior, quotesBehavior, emptyBehavior, errorBehavior);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonQuery otherJsonQuery = (JsonQuery) other;

        return returnedType.equals(otherJsonQuery.returnedType) &&
                outputFormat.equals(otherJsonQuery.outputFormat) &&
                wrapperBehavior == otherJsonQuery.wrapperBehavior &&
                Objects.equals(quotesBehavior, otherJsonQuery.quotesBehavior) &&
                emptyBehavior == otherJsonQuery.emptyBehavior &&
                errorBehavior == otherJsonQuery.errorBehavior;
    }
}
