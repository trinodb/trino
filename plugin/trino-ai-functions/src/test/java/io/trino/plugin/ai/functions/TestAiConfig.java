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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.ai.functions.AiProvider.OPENAI;

class TestAiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AiConfig.class)
                .setProvider(null)
                .setModel(null)
                .setAnalyzeSentimentModel(null)
                .setClassifyModel(null)
                .setExtractModel(null)
                .setFixGrammarModel(null)
                .setGenerateModel(null)
                .setMaskModel(null)
                .setTranslateModel(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ai.provider", "openai")
                .put("ai.model", "model")
                .put("ai.analyze-sentiment.model", "analyzeSentiment")
                .put("ai.classify.model", "classify")
                .put("ai.extract.model", "extract")
                .put("ai.fix-grammar.model", "fixGrammar")
                .put("ai.generate.model", "generate")
                .put("ai.mask.model", "mask")
                .put("ai.translate.model", "translate")
                .buildOrThrow();

        AiConfig expected = new AiConfig()
                .setProvider(OPENAI)
                .setModel("model")
                .setAnalyzeSentimentModel("analyzeSentiment")
                .setClassifyModel("classify")
                .setExtractModel("extract")
                .setFixGrammarModel("fixGrammar")
                .setGenerateModel("generate")
                .setMaskModel("mask")
                .setTranslateModel("translate");

        assertFullMapping(properties, expected);
    }
}
