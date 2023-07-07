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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.VarcharType;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteJsonExtractScalar
        implements ConnectorExpressionRule<Call, MongoExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<ConnectorExpression> JSON_PATH = newCapture();

    private final Pattern<Call> pattern;

    public RewriteJsonExtractScalar()
    {
        this.pattern = call()
                .with(functionName().equalTo(new FunctionName("json_extract_scalar")))
                .with(type().matching(VarcharType.class::isInstance))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().capturedAs(VALUE)))
                // this only captures cases where the JSON_PATH is a literal and no CAST is involved
                .with(argument(1).matching(expression().capturedAs(JSON_PATH)));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<MongoExpression> rewrite(Call call, Captures captures, RewriteContext<MongoExpression> context)
    {
        Optional<MongoExpression> value = context.defaultRewrite(captures.get(VALUE));
        if (value.isEmpty()) {
            return Optional.empty();
        }

        Optional<MongoExpression> jsonPath = context.defaultRewrite(captures.get(JSON_PATH));
        if (jsonPath.isEmpty()) {
            return Optional.empty();
        }
        String jsonPathString = (String) jsonPath.get().expression();

        if (jsonPathString.endsWith("[\"$date\"]")) {
            // { "dateCreated": { "$date": "2003-03-26" } }
            // $date field will be accessible with parent object name
            jsonPathString = jsonPathString.replace("[\"$date\"]", "");
        }

        String jsonColName = (String) value.get().expression();
        List<String> jsonPathSplits = Arrays.stream(jsonPathString.split("\\."))
                .toList();

        Document jsonObjectExpression = new JsonObjectExpressionBuilder(jsonColName, jsonPathSplits)
                .build();

        return Optional.of(new MongoExpression(jsonObjectExpression));
    }
}
