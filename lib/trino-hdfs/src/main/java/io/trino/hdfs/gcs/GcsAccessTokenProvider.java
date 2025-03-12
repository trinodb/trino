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
package io.trino.hdfs.gcs;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.time.Instant;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.concurrent.TimeUnit.HOURS;

public class GcsAccessTokenProvider
        implements AccessTokenProvider
{
    public static final String GCS_ACCESS_TOKEN_CONF = "trino.gcs.oauth-access-token";
    public static final Long EXPIRATION_TIME_MILLISECONDS = HOURS.toMillis(1);
    private Configuration config;

    @Override
    public AccessToken getAccessToken()
    {
        return new AccessToken(nullToEmpty(config.get(GCS_ACCESS_TOKEN_CONF)), Instant.now().plusMillis(EXPIRATION_TIME_MILLISECONDS));
    }

    @Override
    public void refresh() {}

    @Override
    public void setConf(Configuration configuration)
    {
        this.config = configuration;
    }

    @Override
    public Configuration getConf()
    {
        return config;
    }
}
