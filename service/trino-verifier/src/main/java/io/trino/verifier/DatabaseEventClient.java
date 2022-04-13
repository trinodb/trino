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

import io.airlift.event.client.AbstractEventClient;
import io.airlift.json.JsonCodec;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DatabaseEventClient
        extends AbstractEventClient
{
    private final VerifierQueryEventDao dao;
    private final JsonCodec<List<String>> codec;

    @Inject
    public DatabaseEventClient(VerifierQueryEventDao dao, JsonCodec<List<String>> codec)
    {
        this.dao = requireNonNull(dao, "dao is null");
        this.codec = requireNonNull(codec, "codec is null");
    }

    @PostConstruct
    public void postConstruct()
    {
        dao.createTable();
    }

    @Override
    protected <T> void postEvent(T event)
    {
        VerifierQueryEvent queryEvent = (VerifierQueryEvent) event;
        VerifierQueryEventEntity entity = new VerifierQueryEventEntity(
                queryEvent.getSuite(),
                Optional.ofNullable(queryEvent.getRunId()),
                Optional.ofNullable(queryEvent.getSource()),
                Optional.ofNullable(queryEvent.getName()),
                queryEvent.isFailed(),
                Optional.ofNullable(queryEvent.getTestCatalog()),
                Optional.ofNullable(queryEvent.getTestSchema()),
                queryEvent.getTestSetupQueryIds().isEmpty() ? Optional.empty() : Optional.of(codec.toJson(queryEvent.getTestSetupQueryIds())),
                Optional.ofNullable(queryEvent.getTestQueryId()),
                queryEvent.getTestTeardownQueryIds().isEmpty() ? Optional.empty() : Optional.of(codec.toJson(queryEvent.getTestTeardownQueryIds())),
                Optional.ofNullable(queryEvent.getControlCatalog()),
                Optional.ofNullable(queryEvent.getControlSchema()),
                queryEvent.getControlSetupQueryIds().isEmpty() ? Optional.empty() : Optional.of(codec.toJson(queryEvent.getControlSetupQueryIds())),
                Optional.ofNullable(queryEvent.getControlQueryId()),
                queryEvent.getControlTeardownQueryIds().isEmpty() ? Optional.empty() : Optional.of(codec.toJson(queryEvent.getControlTeardownQueryIds())),
                Optional.ofNullable(queryEvent.getErrorMessage()));
        dao.store(entity);
    }
}
