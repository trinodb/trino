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
package io.trino.plugin.lance;

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.util.concurrent.TimeUnit;


public class LanceConfig {
    private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

    /** URL used to access a lancedb via REST client */
    private URI lanceDbUri = URI.create("dummy://db.connect");

    private Duration connectionTimeout = new Duration(1, TimeUnit.MINUTES);

    private int fetchRetryCount;

    @NotEmpty(message = "lanceDb URI cannot be empty")
    public URI getLanceDbUri() {
        return lanceDbUri;
    }

    @Config("lance.uri")
    public LanceConfig setLanceDbUri(String lanceDbUri) {
        this.lanceDbUri = URI.create(lanceDbUri);
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @Config("lance.connection-timeout")
    public LanceConfig setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Integer getFetchRetryCount() {
        return this.fetchRetryCount;
    }

    @Config("lance.connection-retry-count")
    public void setFetchRetryCount(int fetchRetryCount) {
        this.fetchRetryCount = fetchRetryCount;
    }
}
