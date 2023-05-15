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
import io.trino.plugin.session.AbstractSessionPropertyManager;
import io.trino.plugin.session.SessionMatchSpec;
import io.trino.spi.session.SessionConfigurationContext;
import io.trino.spi.session.SessionPropertyConfigurationManager;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SessionPropertyConfigurationManager} implementation that connects to a database for fetching information
 * about session property overrides given {@link SessionConfigurationContext}.
 */
public class DbSessionPropertyManager
        extends AbstractSessionPropertyManager
{
    private final DbSpecsProvider specsProvider;

    @Inject
    public DbSessionPropertyManager(DbSpecsProvider specsProvider)
    {
        this.specsProvider = requireNonNull(specsProvider, "specsProvider is null");
    }

    @Override
    protected List<SessionMatchSpec> getSessionMatchSpecs()
    {
        return ImmutableList.copyOf(specsProvider.get());
    }
}
