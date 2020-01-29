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
package io.prestosql.plugin.hive.s3;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HiveSessionProperties;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.hive.DynamicConfigurationProvider.setCacheKey;
import static io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_IAM_ROLE;

public class S3ConfigurationProvider
        implements DynamicConfigurationProvider
{
    private static final Set<String> SUPPORTED_S3_SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!SUPPORTED_S3_SCHEMES.contains(uri.getScheme())) {
            return;
        }

        Optional<String> iamRole = context.getSession().map(session -> HiveSessionProperties.getS3IamRole(session)).filter(role -> role != null && !role.isEmpty());
        if (iamRole.isPresent()) {
            configuration.set(S3_IAM_ROLE, iamRole.get());
            setCacheKey(configuration, iamRole.get());
        }
    }
}
