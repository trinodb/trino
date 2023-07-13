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
package io.trino.verifier;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.event.client.EventClient;
import org.jdbi.v3.core.Jdbi;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class TrinoVerifierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(VerifierConfig.class);
        eventBinder(binder).bindEventClient(VerifierQueryEvent.class);

        Multibinder<String> supportedClients = newSetBinder(binder, String.class, SupportedEventClients.class);
        supportedClients.addBinding().toInstance("human-readable");
        supportedClients.addBinding().toInstance("file");
        supportedClients.addBinding().toInstance("database");
        Set<String> eventClientTypes = buildConfigObject(VerifierConfig.class).getEventClients();
        bindEventClientClasses(eventClientTypes, binder, newSetBinder(binder, EventClient.class));
    }

    private static void bindEventClientClasses(Set<String> eventClientTypes, Binder binder, Multibinder<EventClient> multibinder)
    {
        for (String eventClientType : eventClientTypes) {
            if (eventClientType.equals("human-readable")) {
                multibinder.addBinding().to(HumanReadableEventClient.class).in(Scopes.SINGLETON);
            }
            else if (eventClientType.equals("file")) {
                multibinder.addBinding().to(JsonEventClient.class).in(Scopes.SINGLETON);
            }
            else if (eventClientType.equals("database")) {
                jsonCodecBinder(binder).bindListJsonCodec(String.class);
                binder.bind(VerifierQueryEventDao.class).toProvider(VerifierQueryEventDaoProvider.class);
                multibinder.addBinding().to(DatabaseEventClient.class).in(Scopes.SINGLETON);
            }
        }
    }

    private static class VerifierQueryEventDaoProvider
            implements Provider<VerifierQueryEventDao>
    {
        private final VerifierQueryEventDao dao;

        @Inject
        public VerifierQueryEventDaoProvider(Jdbi jdbi)
        {
            this.dao = jdbi.onDemand(VerifierQueryEventDao.class);
        }

        @Override
        public VerifierQueryEventDao get()
        {
            return dao;
        }
    }
}
