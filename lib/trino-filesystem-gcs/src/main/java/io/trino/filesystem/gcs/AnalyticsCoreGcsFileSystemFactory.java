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
package io.trino.filesystem.gcs;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;

import java.util.Optional;

public class AnalyticsCoreGcsFileSystemFactory
{
    private final String projectId;
    private final Optional<String> userProject;
    private final Optional<String> decryptionKey;
    private final DataSize channelReadChunkSize;
    private final Optional<String> clientLibToken;
    private final Optional<String> serviceHost;
    private final String userAgent;
    private final GcsFileSystemConfig.FileAccessPattern fileAccessPattern;
    private final boolean footerPrefetchEnabled;
    private final int readThreadCount;

    @Inject
    public AnalyticsCoreGcsFileSystemFactory(GcsFileSystemConfig config)
    {
        projectId = config.getProjectId();
        userProject = config.getUserProject();
        decryptionKey = config.getDecryptionKey();
        channelReadChunkSize = config.getReadBlockSize();
        clientLibToken = config.getClientLibToken();
        serviceHost = config.getEndpoint();
        userAgent = config.getApplicationId();
        fileAccessPattern = config.getAnalyticsCoreFileAccessPattern();
        footerPrefetchEnabled = config.isAnalyticsCoreFooterPrefetchEnabled();
        readThreadCount = config.getAnalyticsCoreReadThreadCount();
    }

    public GcsFileSystem create(Storage storage)
    {
        StorageOptions storageOptions = storage.getOptions();
        ImmutableMap.Builder<String, String> optionMap = ImmutableMap.<String, String>builder()
                .put("gcs.user-agent", userAgent)
                .put("gcs.channel.read.chunk-size-bytes", String.valueOf(channelReadChunkSize.toBytes()))
                .put("gcs.analytics-core.read.file-access-pattern", fileAccessPattern.name())
                .put("gcs.analytics-core.footer.prefetch.enabled", String.valueOf(footerPrefetchEnabled))
                .put("gcs.analytics-core.read.thread.count", String.valueOf(readThreadCount));

        if (projectId != null) {
            optionMap.put("gcs.project-id", projectId);
        }
        else {
            optionMap.put("gcs.project-id", storageOptions.getProjectId());
        }

        decryptionKey.ifPresent(key -> optionMap.put("gcs.decryption-key", key));
        userProject.ifPresent(project -> optionMap.put("gcs.user-project", project));
        serviceHost.ifPresent(host -> optionMap.put("gcs.service-host", host));
        clientLibToken.ifPresent(token -> optionMap.put("gcs.client-lib-token", token));
        GcsAnalyticsCoreOptions options = new GcsAnalyticsCoreOptions("gcs.", optionMap.buildOrThrow());

        return new GcsFileSystemImpl(storage.getOptions().getCredentials(), options.getGcsFileSystemOptions());
    }

    @Override
    public String toString()
    {
        return "AnalyticsCoreGcsFileSystemFactory{" +
                "projectId='" + projectId + '\'' +
                ", userProject=" + userProject +
                ", decryptionKey=" + decryptionKey +
                ", channelReadChunkSize=" + channelReadChunkSize +
                ", clientLibToken=" + clientLibToken +
                ", serviceHost=" + serviceHost +
                ", userAgent='" + userAgent + '\'' +
                ", fileAccessPattern=" + fileAccessPattern +
                ", footerPrefetchEnabled=" + footerPrefetchEnabled +
                ", readThreadCount=" + readThreadCount +
                '}';
    }
}
