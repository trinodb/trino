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
package io.trino.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.inject.Inject;
import io.trino.sql.ExpressionFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;

import java.io.IOException;

import static io.trino.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;

public final class ExpressionSerialization
{
    private ExpressionSerialization() {}

    public static class ExpressionSerializer
            extends JsonSerializer<Expression>
    {
        @Override
        public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeString(ExpressionFormatter.formatExpression(expression));
        }
    }

    public static class ExpressionDeserializer
            extends JsonDeserializer<Expression>
    {
        private final SqlParser sqlParser;

        @Inject
        public ExpressionDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.readValueAs(String.class), new ParsingOptions()));
        }
    }
}
