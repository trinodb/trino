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

public enum ElasticsearchProtocol {
    V6("6.0.0"),
    V7("7.0.0");

    private final String minVersion;
    ElasticsearchProtocol(String minVersion)
    {
        this.minVersion = minVersion;
    }

    public String getMinVersion()
    {
        return minVersion;
    }

    public ElasticsearchProtocolConfig getConfig()
    {
        switch (this) {
            case V6: return new Elasticsearch6ProtocolConfig();
            case V7: return new Elasticsearch7ProtocolConfig();
            default: throw new IllegalArgumentException("unknown protocol config for " + name());
        }
    }
}
