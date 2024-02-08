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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TypeRegistry;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.PlannerContext;
import io.trino.sql.SessionSpecificationEvaluator;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.SessionSpecification;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.InternalTypeManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSessionSpecifications
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final SessionPropertyManager SESSION_PROPERTY_MANAGER = new SessionPropertyManager(ImmutableSet.of(new SystemSessionProperties()), catalogHandle -> Map.of(
            "catalog_property", PropertyMetadata.stringProperty("catalog_property", "Test catalog property", "", false)));

    @Test
    public void testParseSystemSessionProperty()
    {
        assertThatThrownBy(() -> analyze("SESSION invalid_key = 'invalid_value'"))
                .hasMessageContaining("line 1:1: Session property invalid_key does not exist");

        assertThatThrownBy(() -> analyze("SESSION optimize_hash_generation = 'invalid_value'"))
                .hasMessageContaining("Unable to set session property 'optimize_hash_generation' to ''invalid_value'': Cannot cast type varchar(13) to boolean");

        assertThat(analyze("SESSION optimize_hash_generation = true").getSystemProperties())
                .isEqualTo(Map.of("optimize_hash_generation", "true"));

        assertThat(analyze("SESSION optimize_hash_generation = CAST('true' AS boolean)").getSystemProperties())
                .isEqualTo(Map.of("optimize_hash_generation", "true"));

        assertThatThrownBy(() -> analyze("SESSION optimize_hash_generation = true", "SESSION optimize_hash_generation = false"))
                .hasMessageContaining("line 1:1: Session property optimize_hash_generation already set");
    }

    @Test
    public void testCatalogSessionProperty()
    {
        assertThatThrownBy(() -> analyze("SESSION test.invalid_key = 'invalid_value'"))
                .hasMessageContaining("line 1:1: Session property test.invalid_key does not exist");

        assertThatThrownBy(() -> analyze("SESSION test.catalog_property = true"))
                .hasMessageContaining("Unable to set session property 'test.catalog_property' to 'true': Cannot cast type boolean to varchar");

        assertThat(analyze("SESSION test.catalog_property = 'true'").getCatalogProperties("test"))
                .isEqualTo(Map.of("catalog_property", "true"));

        assertThat(analyze("SESSION test.catalog_property = CAST(true AS varchar)").getCatalogProperties("test"))
                .isEqualTo(Map.of("catalog_property", "true"));

        assertThatThrownBy(() -> analyze("SESSION test.catalog_property = 'true'", "SESSION test.catalog_property = 'false'").getCatalogProperties("test"))
                .hasMessageContaining("line 1:1: Session property test.catalog_property already set");
    }

    private static Session analyze(@Language("SQL") String... statements)
    {
        ImmutableList.Builder<SessionSpecification> sessionSpecifications = ImmutableList.builder();
        for (String statement : statements) {
            sessionSpecifications.add(SQL_PARSER.createSessionSpecification(statement));
        }

        TransactionManager transactionManager = new TestingTransactionManager();
        PlannerContext plannerContext = plannerContextBuilder()
                .withMetadata(new MockMetadata())
                .withTransactionManager(transactionManager)
                .build();

        return transaction(transactionManager, plannerContext.getMetadata(), new AllowAllAccessControl())
                .execute(testSession(), transactionSession -> {
                    SessionSpecificationEvaluator evaluator = new SessionSpecificationEvaluator(plannerContext, new AllowAllAccessControl(), SESSION_PROPERTY_MANAGER);
                    return evaluator.prepareSession(transactionSession, sessionSpecifications.build(), Map.of());
                });
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final MetadataManager delegate;

        public MockMetadata()
        {
            FeaturesConfig featuresConfig = new FeaturesConfig();
            TypeOperators typeOperators = new TypeOperators();

            TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
            TypeManager typeManager = new InternalTypeManager(typeRegistry);
            this.delegate = MetadataManager.testMetadataManagerBuilder()
                    .withTypeManager(typeManager)
                    .build();
        }

        @Override
        public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
        {
            return delegate.getCoercion(operatorType, fromType, toType);
        }

        @Override
        public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
        {
            return Optional.of(CatalogHandle.fromId(catalogName + ":NORMAL:v1"));
        }
    }
}
