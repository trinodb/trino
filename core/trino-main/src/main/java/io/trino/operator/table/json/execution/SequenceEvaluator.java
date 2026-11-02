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
package io.trino.operator.table.json.execution;

import com.google.common.collect.ImmutableList;
import io.trino.json.Json;
import io.trino.json.TypedValue;
import io.trino.jsonpath.JsonPathEvaluator;
import io.trino.jsonpath.PathEvaluationException;
import io.trino.operator.scalar.json.JsonOutputConversionException;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.jsonpath.ir.SqlJsonLiteralConverter.getJson;
import static java.lang.String.format;

public final class SequenceEvaluator
{
    private SequenceEvaluator() {}

    // creates a sequence of JSON items, and applies error handling
    public static List<Json> getSequence(Json item, Object[] pathParameters, JsonPathEvaluator pathEvaluator, boolean errorOnError)
    {
        if (item == null) {
            // According to ISO/IEC 9075-2:2016(E) 7.11 <JSON table> p.461 General rules 1) a) empty table should be returned for null input. Empty sequence will result in an empty table.
            return ImmutableList.of();
        }
        // According to ISO/IEC 9075-2:2016(E) 7.11 <JSON table> p.461 General rules 1) e) exception thrown by path evaluation should be handled accordingly to json_table error behavior (ERROR or EMPTY).
        // handle input conversion error for the context item
        if (item.isError()) {
            checkState(!errorOnError, "input conversion error should have been thrown in the input function");
            // the error behavior is EMPTY ON ERROR. Empty sequence will result in an empty table.
            return ImmutableList.of();
        }
        // handle input conversion error for the path parameters
        for (Object parameter : pathParameters) {
            if (parameter instanceof Json json && json.isError()) {
                checkState(!errorOnError, "input conversion error should have been thrown in the input function");
                // the error behavior is EMPTY ON ERROR. Empty sequence will result in an empty table.
                return ImmutableList.of();
            }
        }
        // evaluate path into a sequence
        List<Json> pathResult;
        try {
            pathResult = pathEvaluator.evaluate(item, pathParameters);
        }
        catch (PathEvaluationException e) {
            if (errorOnError) {
                throw e;
            }
            // the error behavior is EMPTY ON ERROR. Empty sequence will result in an empty table.
            return ImmutableList.of();
        }
        // convert sequence to Json items: TypedValue items are encoded back into Json
        ImmutableList.Builder<Json> builder = ImmutableList.builder();
        for (Json element : pathResult) {
            if (element instanceof TypedValue typedValue) {
                Optional<Json> json = getJson(typedValue);
                if (json.isEmpty()) {
                    if (errorOnError) {
                        throw new JsonOutputConversionException(format(
                                "JSON path returned a scalar SQL value of type %s that cannot be represented as JSON",
                                typedValue.type()));
                    }
                    // the error behavior is EMPTY ON ERROR. Empty sequence will result in an empty table.
                    return ImmutableList.of();
                }
                builder.add(json.get());
            }
            else {
                builder.add((Json) element);
            }
        }
        return builder.build();
    }
}
