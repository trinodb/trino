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
package io.trino.plugin.ai.functions;

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class AiFunctions
{
    private final AiClient client;

    @Inject
    public AiFunctions(AiClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @ScalarFunction(value = "ai_analyze_sentiment", deterministic = false)
    @Description("Perform sentiment analysis on text")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiAnalyzeSentiment(@SqlType(StandardTypes.VARCHAR) Slice text)
    {
        return utf8Slice(client.analyzeSentiment(text.toStringUtf8()));
    }

    @ScalarFunction(value = "ai_classify", deterministic = false)
    @Description("Classify text with the provided labels")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiClassify(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType("array(varchar)") Block labels)
    {
        return utf8Slice(client.classify(text.toStringUtf8(), fromSqlArray(labels)));
    }

    @ScalarFunction(value = "ai_extract", deterministic = false)
    @Description("Extract values for the provided labels from text")
    @SqlType("map(varchar,varchar)")
    public SqlMap aiExtract(
            @TypeParameter("map(varchar,varchar)") MapType mapType,
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType("array(varchar)") Block labels)
    {
        return toSqlMap(mapType, client.extract(text.toStringUtf8(), fromSqlArray(labels)));
    }

    @ScalarFunction(value = "ai_fix_grammar", deterministic = false)
    @Description("Correct grammatical errors in text")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiFixGrammar(@SqlType(StandardTypes.VARCHAR) Slice text)
    {
        return utf8Slice(client.fixGrammar(text.toStringUtf8()));
    }

    @ScalarFunction(value = "ai_gen", deterministic = false)
    @Description("Generate text based on a prompt")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiGen(@SqlType(StandardTypes.VARCHAR) Slice prompt)
    {
        return utf8Slice(client.generate(prompt.toStringUtf8()));
    }

    @ScalarFunction(value = "ai_mask", deterministic = false)
    @Description("Mask values for the provided labels in text")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiMask(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType("array(varchar)") Block labels)
    {
        return utf8Slice(client.mask(text.toStringUtf8(), fromSqlArray(labels)));
    }

    @ScalarFunction(value = "ai_translate", deterministic = false)
    @Description("Translate text to the specified language")
    @SqlType(StandardTypes.VARCHAR)
    public Slice aiTranslate(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType(StandardTypes.VARCHAR) Slice language)
    {
        return utf8Slice(client.translate(text.toStringUtf8(), language.toStringUtf8()));
    }

    private static List<String> fromSqlArray(Block block)
    {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            list.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return list;
    }

    private static SqlMap toSqlMap(MapType type, Map<String, String> map)
    {
        return buildMapValue(type, map.size(), (keyBuilder, valueBuilder) ->
                map.forEach((key, value) -> {
                    VARCHAR.writeSlice(keyBuilder, utf8Slice(key));
                    if (value == null) {
                        valueBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(valueBuilder, utf8Slice(value));
                    }
                }));
    }
}
