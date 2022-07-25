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
package io.trino.plugin.resourcegroups.db;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class DaoProvider
        implements Provider<ResourceGroupsDao>
{
    private final ResourceGroupsDao dao;

    @Inject
    public DaoProvider(DbResourceGroupConfig config, Jdbi jdbi)
    {
        requireNonNull(config, "config is null");
        this.dao = jdbi
                .installPlugin(new SqlObjectPlugin())
                .onDemand(ResourceGroupsDao.class);
    }

    @Override
    public ResourceGroupsDao get()
    {
        return dao;
    }
}
