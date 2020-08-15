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

import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.MetadataManager.TestMetadataManagerBuilder;
import io.trino.metadata.SqlFunction;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.PlannerContext;
import io.trino.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
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
            TypeManager typeManager = TESTING_TYPE_MANAGER;

            Metadata metadata = this.metadata;
            if (metadata == null) {
                TestMetadataManagerBuilder builder = MetadataManager.testMetadataManagerBuilder()
                        .withTypeManager(typeManager);
                if (transactionManager != null) {
                    builder.withTransactionManager(transactionManager);
                }
                metadata = builder.build();
            }
            metadata.addFunctions(functions);

            return new PlannerContext(
                    metadata,
                    new TypeOperators(),
                    new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager),
                    typeManager);
        }
    }
}
