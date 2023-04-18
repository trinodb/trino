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
package io.trino.plugin.pinot.deepstore.gcs;

import io.airlift.configuration.Config;

import java.util.Optional;

public class PinotGcsConfig
{
    private Optional<String> gcpProjectId = Optional.empty();
    private Optional<String> gcpKey = Optional.empty();

    public Optional<String> getGcpProjectId()
    {
        return gcpProjectId;
    }

    @Config("pinot.gcp-project-id")
    public PinotGcsConfig setGcpProjectId(String gcpProjectId)
    {
        this.gcpProjectId = Optional.ofNullable(gcpProjectId);
        return this;
    }

    public Optional<String> getGcpKey()
    {
        return gcpKey;
    }

    @Config("pinot.gcp-key")
    public PinotGcsConfig setGcpKey(String gcpKey)
    {
        this.gcpKey = Optional.ofNullable(gcpKey);
        return this;
    }
}
