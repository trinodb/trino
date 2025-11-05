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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class AiConfig
{
    private AiProvider provider;
    private String model;
    private String analyzeSentimentModel;
    private String classifyModel;
    private String extractModel;
    private String fixGrammarModel;
    private String generateModel;
    private String maskModel;
    private String translateModel;

    @NotNull
    public AiProvider getProvider()
    {
        return provider;
    }

    @Config("ai.provider")
    public AiConfig setProvider(AiProvider provider)
    {
        this.provider = provider;
        return this;
    }

    @NotEmpty
    public String getModel()
    {
        return model;
    }

    @Config("ai.model")
    public AiConfig setModel(String model)
    {
        this.model = model;
        return this;
    }

    public String getAnalyzeSentimentModel()
    {
        return analyzeSentimentModel;
    }

    @Config("ai.analyze-sentiment.model")
    public AiConfig setAnalyzeSentimentModel(String analyzeSentimentModel)
    {
        this.analyzeSentimentModel = analyzeSentimentModel;
        return this;
    }

    public String getClassifyModel()
    {
        return classifyModel;
    }

    @Config("ai.classify.model")
    public AiConfig setClassifyModel(String classifyModel)
    {
        this.classifyModel = classifyModel;
        return this;
    }

    public String getExtractModel()
    {
        return extractModel;
    }

    @Config("ai.extract.model")
    public AiConfig setExtractModel(String extractModel)
    {
        this.extractModel = extractModel;
        return this;
    }

    public String getFixGrammarModel()
    {
        return fixGrammarModel;
    }

    @Config("ai.fix-grammar.model")
    public AiConfig setFixGrammarModel(String fixGrammarModel)
    {
        this.fixGrammarModel = fixGrammarModel;
        return this;
    }

    public String getGenerateModel()
    {
        return generateModel;
    }

    @Config("ai.generate.model")
    public AiConfig setGenerateModel(String generateModel)
    {
        this.generateModel = generateModel;
        return this;
    }

    public String getMaskModel()
    {
        return maskModel;
    }

    @Config("ai.mask.model")
    public AiConfig setMaskModel(String maskModel)
    {
        this.maskModel = maskModel;
        return this;
    }

    public String getTranslateModel()
    {
        return translateModel;
    }

    @Config("ai.translate.model")
    public AiConfig setTranslateModel(String translateModel)
    {
        this.translateModel = translateModel;
        return this;
    }
}
