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
package io.trino.hdfs.cos;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.trino.hdfs.DynamicConfigurationProvider.setCacheKey;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;

public class CosServiceConfigurationProvider
        implements DynamicConfigurationProvider
{
    private final Map<String, ServiceConfig> services;

    @Inject
    public CosServiceConfigurationProvider(HiveCosServiceConfig config)
    {
        services = ServiceConfig.loadServiceConfigs(config.getServiceConfig());
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!"cos".equals(uri.getScheme())) {
            return;
        }

        List<String> parts = Splitter.on('.').limit(2).splitToList(uri.getHost());
        if (parts.size() != 2) {
            return;
        }
        String serviceName = parts.get(1);

        ServiceConfig service = services.get(serviceName);
        if (service == null) {
            return;
        }

        configuration.set(S3_ACCESS_KEY, service.getAccessKey());
        configuration.set(S3_SECRET_KEY, service.getSecretKey());
        service.getEndpoint().ifPresent(endpoint -> configuration.set(S3_ENDPOINT, endpoint));
        setCacheKey(configuration, serviceName);
    }
}
