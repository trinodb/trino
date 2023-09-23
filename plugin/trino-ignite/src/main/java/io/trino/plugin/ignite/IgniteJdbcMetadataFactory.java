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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IgniteJdbcMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public IgniteJdbcMetadataFactory(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, jdbcQueryEventListeners);
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "jdbcQueryEventListeners is null"));
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new IgniteMetadata(transactionCachingJdbcClient, jdbcQueryEventListeners);
    }
}
