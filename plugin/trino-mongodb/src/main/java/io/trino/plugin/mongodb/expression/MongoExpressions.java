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
package io.trino.plugin.mongodb.expression;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.VarcharType;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class MongoExpressions
{
    private MongoExpressions() {}

    public static Document documentOf(String key, Object value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        return new Document(key, value);
    }

    public static Document toDecimal(Object value)
    {
        requireNonNull(value, "value is null");
        return documentOf("$toDecimal", value);
    }

    public static Document toString(Object value)
    {
        requireNonNull(value, "value is null");
        Document convertValue = documentOf("input", value)
                .append("to", "string")
                .append("onError", null);
        return documentOf("$convert", convertValue);
    }

    public static Optional<List<Document>> buildVariableExpression(FilterExpression filterExpression)
    {
        requireNonNull(filterExpression, "filterExpression is null");
        checkArgument(filterExpression.isVariable(), "Filter expression is not variable");
        checkArgument(filterExpression.expressionInfo().isPresent(), "expressionInfo of variable is not present");
        ExpressionInfo expressionInfo = filterExpression.expressionInfo().get();
        Optional<String> mongoType = expressionInfo.getMongoType();
        // If mapping between trino type and mongo type does not happen, then pushdown won't be supported
        if (mongoType.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<Document> expressionBuilder = ImmutableList.builder();
        // When using aggregate operators, if the field being compared either does not exist in the document or has a null value,
        // comparison operators include those documents in the results. To address this, comparing operands greater than null
        // ensures that the output excludes documents where the compared field is either missing or has a null value.
        // https://www.mongodb.com/community/forums/t/why-aggregate-operators-behave-differently-than-projection-operators/237219
        expressionBuilder.add(documentOf("$gt", Arrays.asList(filterExpression.expression(), null)));

        // Handle mismatched data type value of documents by matching the data type of column and document value
        if (expressionInfo.getType() == BIGINT) {
            expressionBuilder.add(documentOf("$in", ImmutableList.of(documentOf("$type", expressionInfo.getName()), ImmutableList.of(mongoType.get(), "int"))));
        }
        else if (!(expressionInfo.getType() instanceof VarcharType)) {
            expressionBuilder.add(documentOf("$eq", ImmutableList.of(documentOf("$type", expressionInfo.getName()), mongoType.get())));
        }
        return Optional.of(expressionBuilder.build());
    }

    public static boolean isVarchar(FilterExpression filterExpression)
    {
        requireNonNull(filterExpression, "filterExpression is null");
        if (!filterExpression.isVariable()) {
            return false;
        }
        verify(filterExpression.expressionInfo().isPresent(), "expressionInfo of variable is not present");
        return filterExpression.expressionInfo().get().getType() instanceof VarcharType;
    }
}
