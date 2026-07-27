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

import io.trino.spi.security.ConnectorIdentity;

import java.util.List;
import java.util.Map;

public interface AiClient
{
    String analyzeSentiment(ConnectorIdentity identity, String text);

    String classify(ConnectorIdentity identity, String text, List<String> labels);

    Map<String, String> extract(ConnectorIdentity identity, String text, List<String> labels);

    String fixGrammar(ConnectorIdentity identity, String text);

    String generate(ConnectorIdentity identity, String prompt);

    String mask(ConnectorIdentity identity, String text, List<String> labels);

    String translate(ConnectorIdentity identity, String text, String language);
}
