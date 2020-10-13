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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class KafkaSecurityModules
{
    public static final TypeLiteral<Map<String, Object>> STRING_TO_OBJECT_MAP = new TypeLiteral<>() {};

    private KafkaSecurityModules() {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForKafkaSecurity {}

    public static class KafkaSslSecurityModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(KafkaSecurityConfig.class);
        }

        @ForKafkaSecurity
        @Provides
        public Map<String, Object> getSecurityKafkaClientProperties(KafkaSecurityConfig securityConfig)
        {
            return ImmutableMap.copyOf(securityConfig.getKafkaClientProperties());
        }
    }

    public static class KafkaPlaintextSecurityModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(STRING_TO_OBJECT_MAP).annotatedWith(ForKafkaSecurity.class).toInstance(ImmutableMap.of());
        }
    }
}
