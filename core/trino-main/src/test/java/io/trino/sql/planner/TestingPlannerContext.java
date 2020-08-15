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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.FeaturesConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.MetadataManager.TestMetadataManagerBuilder;
import io.trino.metadata.SqlFunction;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.PlannerContext;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static java.util.Objects.requireNonNull;

public final class TestingPlannerContext
{
    public static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder().build();

    private TestingPlannerContext() {}

    public static Builder plannerContextBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Metadata metadata;
        private TransactionManager transactionManager;
        private final List<Type> types = new ArrayList<>();
        private final List<ParametricType> parametricTypes = new ArrayList<>();
        private final List<SqlFunction> functions = new ArrayList<>();

        private Builder() {}

        public Builder withMetadata(Metadata metadata)
        {
            checkState(this.metadata == null, "metadata already set");
            checkState(this.transactionManager == null, "transactionManager already set");
            this.metadata = requireNonNull(metadata, "metadata is null");
            return this;
        }

        public Builder withTransactionManager(TransactionManager transactionManager)
        {
            checkState(this.metadata == null, "metadata already set");
            checkState(this.transactionManager == null, "transactionManager already set");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            return this;
        }

        public Builder addType(Type type)
        {
            types.add(type);
            return this;
        }

        public Builder addParametricType(ParametricType parametricType)
        {
            parametricTypes.add(parametricType);
            return this;
        }

        public Builder addFunctions(Set<Class<?>> functionClasses)
        {
            return addFunctions(extractFunctions(functionClasses));
        }

        public Builder addFunctions(List<? extends SqlFunction> sqlFunctions)
        {
            functions.addAll(sqlFunctions);
            return this;
        }

        public PlannerContext build()
        {
            FeaturesConfig featuresConfig = new FeaturesConfig();
            TypeOperators typeOperators = new TypeOperators();

            TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
            TypeManager typeManager = new InternalTypeManager(typeRegistry);
            types.forEach(typeRegistry::addType);
            parametricTypes.forEach(typeRegistry::addParametricType);

            GlobalFunctionCatalog globalFunctionCatalog = new GlobalFunctionCatalog(featuresConfig, typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN);
            globalFunctionCatalog.addFunctions(functions);

            BlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager);
            globalFunctionCatalog.addFunctions(ImmutableList.of(new LiteralFunction(blockEncodingSerde)));

            Metadata metadata = this.metadata;
            if (metadata == null) {
                TestMetadataManagerBuilder builder = MetadataManager.testMetadataManagerBuilder()
                        .withFeaturesConfig(featuresConfig)
                        .withTypeManager(typeManager)
                        .withGlobalFunctionCatalog(globalFunctionCatalog);
                if (transactionManager != null) {
                    builder.withTransactionManager(transactionManager);
                }
                metadata = builder.build();
            }

            return new PlannerContext(
                    metadata,
                    typeOperators,
                    blockEncodingSerde,
                    typeManager,
                    new FunctionManager(globalFunctionCatalog));
        }
    }
}
