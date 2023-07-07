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

import org.bson.Document;

import java.util.List;

import static io.trino.plugin.mongodb.expression.ExpressionUtils.documentOf;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class JsonObjectExpressionBuilder
{
    private final String jsonVariable;
    private final List<String> jsonPath;

    public JsonObjectExpressionBuilder(String jsonVariable, List<String> jsonPath)
    {
        this.jsonVariable = requireNonNull(jsonVariable, "jsonVariable is null");
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
    }

    public Document build()
    {
        return buildExpression(jsonVariable, jsonPath, 0);
    }

    private static Document buildExpression(String variable, List<String> jsonPath, int variableNumber)
    {
        StringBuilder variableBuilder = new StringBuilder(variable);
        for (int i = 0; i < jsonPath.size(); i++) {
            String path = jsonPath.get(i);
            if (isInteger(path)) {
                String letVariable = newVariable(variableNumber);
                Document inExpr = inExpression(letVariable, jsonPath.subList(i + 1, jsonPath.size()), variableNumber + 1);
                return letExpression(letVariable, variableBuilder.toString(), Integer.parseInt(path), inExpr);
            }
            else {
                variableBuilder.append(".").append(path);
            }
        }
        return ExpressionUtils.toString(variableBuilder.toString());
    }

    private static Document inExpression(String letVariable, List<String> path, int variableNumber)
    {
        return buildExpression("$$" + letVariable, path, variableNumber);
    }

    private static Document letExpression(String letVariable, String variable, int index, Document inExpression)
    {
        Document letValue = documentOf("vars", documentOf(letVariable, arrayObjectConditionExpression(variable, index)))
                .append("in", inExpression);
        return documentOf("$let", letValue);
    }

    private static Document arrayObjectConditionExpression(String variable, int index)
    {
        return documentOf("$cond", asList(arrayCheck(variable), arrayElemAt(variable, index), objectElement(variable, index)));
    }

    private static Document arrayCheck(String variable)
    {
        Document type = documentOf("$type", variable);
        return documentOf("$eq", asList(type, "array"));
    }

    private static Document arrayElemAt(String variable, int index)
    {
        return documentOf("$arrayElemAt", asList(variable, index));
    }

    private static String objectElement(String variable, int index)
    {
        return "%s.%d".formatted(variable, index);
    }

    private static String newVariable(int variableNumber)
    {
        return "var_%05d".formatted(variableNumber);
    }

    private static boolean isInteger(String value)
    {
        try {
            Integer.parseInt(value);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
}
