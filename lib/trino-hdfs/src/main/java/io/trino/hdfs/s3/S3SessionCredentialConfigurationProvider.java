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
package io.trino.hdfs.s3;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Map;

import static com.amazonaws.auth.profile.internal.ProfileKeyConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.auth.profile.internal.ProfileKeyConstants.AWS_SECRET_ACCESS_KEY;
import static com.amazonaws.auth.profile.internal.ProfileKeyConstants.AWS_SESSION_TOKEN;
import static io.trino.hdfs.DynamicConfigurationProvider.setCacheKey;
import static io.trino.hdfs.s3.S3SecurityMappingConfigurationProvider.SCHEMES;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static java.nio.charset.StandardCharsets.UTF_8;

public class S3SessionCredentialConfigurationProvider
        implements DynamicConfigurationProvider
{
    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!SCHEMES.contains(uri.getScheme())) {
            return;
        }

        Hasher hasher = Hashing.sha256().newHasher();

        Map<String, String> extraCredentials = context.getIdentity().getExtraCredentials();

        if (extraCredentials.containsKey(AWS_ACCESS_KEY_ID)) {
            configuration.set(S3_ACCESS_KEY, extraCredentials.get(AWS_ACCESS_KEY_ID));
            hasher.putString(extraCredentials.get(AWS_ACCESS_KEY_ID), UTF_8);
        }

        if (extraCredentials.containsKey(AWS_SECRET_ACCESS_KEY)) {
            configuration.set(S3_SECRET_KEY, extraCredentials.get(AWS_SECRET_ACCESS_KEY));
            hasher.putString(extraCredentials.get(AWS_SECRET_ACCESS_KEY), UTF_8);
        }

        if (extraCredentials.containsKey(AWS_SESSION_TOKEN)) {
            configuration.set(S3_SESSION_TOKEN, extraCredentials.get(AWS_SESSION_TOKEN));
            hasher.putString(extraCredentials.get(AWS_SESSION_TOKEN), UTF_8);
        }

        setCacheKey(configuration, hasher.hash().toString());
    }
}
