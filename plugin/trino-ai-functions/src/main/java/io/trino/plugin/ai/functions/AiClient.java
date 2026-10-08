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

import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Map;

public interface AiClient
{
    String analyzeSentiment(ConnectorSession session, String text);

    String classify(ConnectorSession session, String text, List<String> labels);

    Map<String, String> extract(ConnectorSession session, String text, List<String> labels);

    String fixGrammar(ConnectorSession session, String text);

    String generate(ConnectorSession session, String prompt);

    String mask(ConnectorSession session, String text, List<String> labels);

    String translate(ConnectorSession session, String text, String language);
}
