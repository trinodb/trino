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
package io.trino.server.security.oauth2;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.nimbusds.jose.KeyLengthException;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.client.NodeVersion;

import java.security.NoSuchAlgorithmException;
import java.time.Clock;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class JweTokenSerializerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RefreshTokensConfig.class);
    }

    @Provides
    @Singleton
    @Inject
    public TokenPairSerializer getTokenPairSerializer(
            OAuth2Client client,
            NodeVersion nodeVersion,
            RefreshTokensConfig config,
            OAuth2Config oAuth2Config)
            throws KeyLengthException, NoSuchAlgorithmException
    {
        return new JweTokenSerializer(
                config,
                client,
                config.getIssuer() + "_" + nodeVersion.getVersion(),
                config.getAudience(),
                oAuth2Config.getPrincipalField(),
                Clock.systemUTC(),
                config.getTokenExpiration());
    }
}
