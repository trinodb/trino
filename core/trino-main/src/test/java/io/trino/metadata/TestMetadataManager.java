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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.FeaturesConfig;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.parser.SqlParser;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;

import java.util.Set;

import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;

public final class TestMetadataManager
{
    private TestMetadataManager() {}

    public static MetadataManager createTestMetadataManager()
    {
        return builder().build();
    }

    public static TestMetadataManager.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private TransactionManager transactionManager;
        private TypeManager typeManager = TESTING_TYPE_MANAGER;
        private GlobalFunctionCatalog globalFunctionCatalog;
        private LanguageFunctionManager languageFunctionManager;

        private Builder() {}

        public Builder withTransactionManager(TransactionManager transactionManager)
        {
            this.transactionManager = transactionManager;
            return this;
        }

        public Builder withTypeManager(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            return this;
        }

        public Builder withGlobalFunctionCatalog(GlobalFunctionCatalog globalFunctionCatalog)
        {
            this.globalFunctionCatalog = globalFunctionCatalog;
            return this;
        }

        public Builder withLanguageFunctionManager(LanguageFunctionManager languageFunctionManager)
        {
            this.languageFunctionManager = languageFunctionManager;
            return this;
        }

        public MetadataManager build()
        {
            TransactionManager transactionManager = this.transactionManager;
            if (transactionManager == null) {
                transactionManager = createTestTransactionManager();
            }

            GlobalFunctionCatalog globalFunctionCatalog = this.globalFunctionCatalog;
            if (globalFunctionCatalog == null) {
                globalFunctionCatalog = new GlobalFunctionCatalog(
                        () -> { throw new UnsupportedOperationException(); },
                        () -> { throw new UnsupportedOperationException(); },
                        () -> { throw new UnsupportedOperationException(); });
                TypeOperators typeOperators = new TypeOperators();
                globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN));
            }

            if (languageFunctionManager == null) {
                BlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager);
                LanguageFunctionEngineManager engineManager = new LanguageFunctionEngineManager();
                languageFunctionManager = new LanguageFunctionManager(new SqlParser(), typeManager, _ -> ImmutableSet.of(), blockEncodingSerde, engineManager);
            }

            TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(_ -> new CatalogTableFunctions(ImmutableList.of()));

            return new MetadataManager(
                    new AllowAllAccessControl(),
                    new SecurityMetadata(),
                    transactionManager,
                    globalFunctionCatalog,
                    languageFunctionManager,
                    tableFunctionRegistry,
                    typeManager);
        }
    }

    private static class SecurityMetadata
            extends DisabledSystemSecurityMetadata
    {
        @Override
        public Set<String> listEnabledRoles(Identity identity)
        {
            return ImmutableSet.of("system-role");
        }
    }
}
