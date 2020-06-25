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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.PrestoException;

import java.util.Properties;

import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class KafkaProducerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        try {
            binder.bind(Properties.class).toInstance(new Properties());
            binder.bind(PlainTextKafkaProducerFactory.class).toConstructor(PlainTextKafkaProducerFactory.class.getConstructor(KafkaConfig.class, Properties.class)).in(Scopes.SINGLETON);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(GENERIC_USER_ERROR, e);
        }
    }
}
