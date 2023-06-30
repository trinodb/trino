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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.client.NodeVersion;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSetSessionTask
{
    private static final String CATALOG_NAME = "my_catalog";
    private static final String MUST_BE_POSITIVE = "property must be positive";

    private enum Size
    {
        SMALL,
        MEDIUM,
        LARGE,
    }

    private LocalQueryRunner queryRunner;
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;
    private PlannerContext plannerContext;
    private SessionPropertyManager sessionPropertyManager;
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.builder(TEST_SESSION)
                .withExtraSystemSessionProperties(ImmutableSet.of(() -> ImmutableList.of(
                        stringProperty(
                                "foo",
                                "test property",
                                null,
                                false))))
                .build();

        queryRunner.createCatalog(
                CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withSessionProperty(stringProperty(
                                "bar",
                                "test property",
                                null,
                                false))
                        .withSessionProperty(integerProperty(
                                "positive_property",
                                "property that should be positive",
                                null,
                                TestSetSessionTask::validatePositive,
                                false))
                        .withSessionProperty(enumProperty(
                                "size_property",
                                "size enum property",
                                Size.class,
                                null,
                                false))
                        .build(),
                ImmutableMap.of());

        transactionManager = queryRunner.getTransactionManager();
        accessControl = queryRunner.getAccessControl();
        metadata = queryRunner.getMetadata();
        plannerContext = queryRunner.getPlannerContext();
        sessionPropertyManager = queryRunner.getSessionPropertyManager();
    }

    private static void validatePositive(Object value)
    {
        int intValue = ((Number) value).intValue();
        if (intValue < 0) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, MUST_BE_POSITIVE);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        executor.shutdownNow();
        executor = null;
        transactionManager = null;
        accessControl = null;
        metadata = null;
        plannerContext = null;
        sessionPropertyManager = null;
    }

    @Test
    public void testSetSession()
    {
        testSetSession("bar", new StringLiteral("baz"), "baz");
        testSetSession("bar",
                new TestingFunctionResolution(transactionManager, plannerContext)
                        .functionCallBuilder(QualifiedName.of("concat"))
                        .addArgument(VARCHAR, new StringLiteral("ban"))
                        .addArgument(VARCHAR, new StringLiteral("ana"))
                        .build(),
                "banana");
    }

    @Test
    public void testSetSessionWithValidation()
    {
        testSetSession("positive_property", new LongLiteral("0"), "0");
        testSetSession("positive_property", new LongLiteral("2"), "2");

        assertThatThrownBy(() -> testSetSession("positive_property", new LongLiteral("-1"), "-1"))
                .isInstanceOf(TrinoException.class)
                .hasMessage(MUST_BE_POSITIVE);
    }

    @Test
    public void testSetSessionWithInvalidEnum()
    {
        assertThatThrownBy(() -> testSetSession("size_property", new StringLiteral("XL"), "XL"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid value [XL]. Valid values: [SMALL, MEDIUM, LARGE]")
                .matches(throwable -> ((TrinoException) throwable).getErrorCode() == INVALID_SESSION_PROPERTY.toErrorCode());
    }

    @Test
    public void testSetSessionWithParameters()
    {
        FunctionCall functionCall = new TestingFunctionResolution(transactionManager, plannerContext)
                .functionCallBuilder(QualifiedName.of("concat"))
                .addArgument(VARCHAR, new StringLiteral("ban"))
                .addArgument(VARCHAR, new Parameter(0))
                .build();
        testSetSessionWithParameters("bar", functionCall, "banana", ImmutableList.of(new StringLiteral("ana")));
    }

    private void testSetSession(String property, Expression expression, String expectedValue)
    {
        testSetSessionWithParameters(property, expression, expectedValue, emptyList());
    }

    private void testSetSessionWithParameters(String property, Expression expression, String expectedValue, List<Expression> parameters)
    {
        QualifiedName qualifiedPropName = QualifiedName.of(CATALOG_NAME, property);
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                Optional.empty(),
                format("set %s = 'old_value'", qualifiedPropName),
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
        getFutureValue(new SetSessionTask(plannerContext, accessControl, sessionPropertyManager).execute(new SetSession(qualifiedPropName, expression), stateMachine, parameters, WarningCollector.NOOP));

        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties, ImmutableMap.of(qualifiedPropName.toString(), expectedValue));
    }
}
