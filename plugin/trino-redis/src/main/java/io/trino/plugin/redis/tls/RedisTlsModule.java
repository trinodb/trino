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
package io.trino.plugin.redis.tls;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.plugin.redis.RedisClientConfigurator;
import redis.clients.jedis.SslOptions;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RedisTlsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RedisTlsConfig.class);
    }

    @ProvidesIntoSet
    @Singleton
    public RedisClientConfigurator tlsConfigurator(RedisTlsConfig config)
    {
        return builder -> builder.sslOptions(buildSslOptions(config));
    }

    private static SslOptions buildSslOptions(RedisTlsConfig config)
    {
        SslOptions.Builder optionsBuilder = SslOptions.builder();
        config.getKeystorePath().ifPresent(path ->
                optionsBuilder.keystore(path, config.getKeystorePassword().map(String::toCharArray).orElse(null)));
        config.getTruststorePath().ifPresent(path ->
                optionsBuilder.truststore(path, config.getTruststorePassword().map(String::toCharArray).orElse(null)));
        return optionsBuilder.build();
    }
}
