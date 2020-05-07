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
package io.prestosql.elasticsearch.client.protocols;

import java.util.stream.Stream;

public enum ElasticsearchProtocol {
    V6("6"),
    V7("7");

    private final String serie;
    ElasticsearchProtocol(String serie)
    {
        this.serie = serie;
    }

    public String getMinVersion()
    {
        return String.format("%s.0.0", serie);
    }

    public ElasticsearchProtocolConfig getConfig()
    {
        switch (this) {
            case V6: return new Elasticsearch6ProtocolConfig();
            case V7: return new Elasticsearch7ProtocolConfig();
            default: throw new IllegalArgumentException("unknown protocol config for " + name());
        }
    }

    public static ElasticsearchProtocol fromVersion(String version) {
        String serie = version.replaceAll("^(\\d+?)\\.(\\d+?)\\.(\\d+?)$", "$1");
        return Stream.of(ElasticsearchProtocol.values())
                .filter(protocol -> protocol.serie.equals(serie))
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }
}
