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
package io.trino.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.connector.ConnectorAccessControl;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class SqlStandardSecurityModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorAccessControl.class).to(SqlStandardAccessControl.class).in(Scopes.SINGLETON);
        binder.bind(AccessControlMetadataFactory.class).to(SqlStandardAccessControlMetadataFactory.class);
        newOptionalBinder(binder, SqlStandardAccessControlMetastore.class).setDefault().to(SemiTransactionalSqlStandardAccessControlMetastore.class).in(Scopes.SINGLETON);
    }

    private static final class SqlStandardAccessControlMetadataFactory
            implements AccessControlMetadataFactory
    {
        public SqlStandardAccessControlMetadataFactory() {}

        @Override
        public AccessControlMetadata create(SqlStandardAccessControlMetadataMetastore metastore)
        {
            return new SqlStandardAccessControlMetadata(metastore);
        }
    }
}
