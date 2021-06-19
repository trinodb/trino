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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.StringLiteral;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSetSessionTask
{
    private static final String CATALOG_NAME = "foo";
    private static final String MUST_BE_POSITIVE = "property must be positive";

    private enum Size
    {
        SMALL,
        MEDIUM,
        LARGE,
    }

    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;

    public TestSetSessionTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        metadata.getSessionPropertyManager().addSystemSessionProperty(stringProperty(
                CATALOG_NAME,
                "test property",
                null,
                false));

        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);

        List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
                stringProperty(
                        "bar",
                        "test property",
                        null,
                        false),
                integerProperty(
                        "positive_property",
                        "property that should be positive",
                        null,
                        TestSetSessionTask::validatePositive,
                        false),
                enumProperty(
                        "size_property",
                        "size enum property",
                        Size.class,
                        null,
                        false));

        metadata.getSessionPropertyManager().addConnectorSessionProperties(bogusTestingCatalog.getConnectorCatalogName(), sessionProperties);

        catalogManager.registerCatalog(bogusTestingCatalog);
    }

    private static void validatePositive(Object value)
    {
        int intValue = ((Number) value).intValue();
        if (intValue < 0) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, MUST_BE_POSITIVE);
        }
    }

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testSetSession()
    {
        testSetSession("bar", new StringLiteral("baz"), "baz");
        testSetSession("bar",
                new FunctionCallBuilder(metadata)
                        .setName(QualifiedName.of("concat"))
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
        FunctionCall functionCall = new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of("concat"))
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
                Optional.empty());
        getFutureValue(new SetSessionTask().execute(new SetSession(qualifiedPropName, expression), transactionManager, metadata, accessControl, stateMachine, parameters, WarningCollector.NOOP));

        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties, ImmutableMap.of(qualifiedPropName.toString(), expectedValue));
    }
}
