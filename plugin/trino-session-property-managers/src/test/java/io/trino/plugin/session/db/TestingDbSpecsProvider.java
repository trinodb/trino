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
package io.trino.plugin.session.db;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.session.SessionMatchSpec;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Test implementation of DbSpecsProvider.
 * {@link SessionPropertiesDao#getSessionMatchSpecs} is invoked every time the get() method is called.
 */
public class TestingDbSpecsProvider
        implements DbSpecsProvider
{
    private static final Logger log = Logger.get(TestingDbSpecsProvider.class);

    private final AtomicReference<List<SessionMatchSpec>> sessionMatchSpecs = new AtomicReference<>(ImmutableList.of());
    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    private final SessionPropertiesDao dao;

    @Inject
    public TestingDbSpecsProvider(SessionPropertiesDao dao)
    {
        requireNonNull(dao, "dao is null");
        this.dao = dao;

        dao.createSessionSpecsTable();
        dao.createSessionClientTagsTable();
        dao.createSessionPropertiesTable();
    }

    @PreDestroy
    public void destroy()
    {
        destroyed.compareAndSet(false, true);
    }

    @Override
    public List<SessionMatchSpec> get()
    {
        checkState(!destroyed.get(), "provider already destroyed");

        try {
            sessionMatchSpecs.set(dao.getSessionMatchSpecs());
        }
        catch (RuntimeException e) {
            // Swallow exceptions
            log.error(e, "Error reloading configuration");
        }
        return ImmutableList.copyOf(this.sessionMatchSpecs.get());
    }
}
