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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class PrestoS3ProfileCredentialsProvider
        implements AWSCredentialsProvider
{
    public static final String S3_PROFILE_CREDENTIALS_PROVIDER_PROFILE = "presto.s3.profile-credentials-provider-profile";

    private final ProfileCredentialsProvider delegate;

    public PrestoS3ProfileCredentialsProvider(URI uri, Configuration conf)
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");
        String profile = conf.get(S3_PROFILE_CREDENTIALS_PROVIDER_PROFILE);
        delegate = new ProfileCredentialsProvider(profile);
    }

    @Override
    public AWSCredentials getCredentials()
    {
        return delegate.getCredentials();
    }

    @Override
    public void refresh()
    {
        delegate.refresh();
    }
}
