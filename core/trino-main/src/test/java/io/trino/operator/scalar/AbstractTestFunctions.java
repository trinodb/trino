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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.metadata.FunctionListBuilder;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Plugin;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.Signature.mangleOperatorName;
import static io.trino.operator.scalar.timestamp.VarcharToTimestampCast.castToLongTimestamp;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public abstract class AbstractTestFunctions
{
    protected final Session session;
    private final FeaturesConfig config;
    protected FunctionAssertions functionAssertions;

    protected AbstractTestFunctions()
    {
        this(TEST_SESSION);
    }

    protected AbstractTestFunctions(Session session)
    {
        this(session, new FeaturesConfig());
    }

    protected AbstractTestFunctions(FeaturesConfig config)
    {
        this(TEST_SESSION, config);
    }

    protected AbstractTestFunctions(Session session, FeaturesConfig config)
    {
        this.session = requireNonNull(session, "session is null");
        this.config = requireNonNull(config, "config is null");
    }

    @BeforeClass
    public final void initTestFunctions()
    {
        functionAssertions = new FunctionAssertions(session, config);
    }

    @AfterClass(alwaysRun = true)
    public final void destroyTestFunctions()
    {
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    protected void assertFunction(@Language("SQL") String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    protected void assertOperator(OperatorType operator, String value, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(format("\"%s\"(%s)", mangleOperatorName(operator), value), expectedType, expected);
    }

    protected void assertDecimalFunction(@Language("SQL") String statement, SqlDecimal expectedResult)
    {
        assertFunction(
                statement,
                createDecimalType(expectedResult.getPrecision(), expectedResult.getScale()),
                expectedResult);
    }

    protected void assertInvalidFunction(@Language("SQL") String projection, ErrorCodeSupplier errorCode, String message)
    {
        functionAssertions.assertInvalidFunction(projection, errorCode, message);
    }

    protected void assertInvalidFunction(@Language("SQL") String projection, String message)
    {
        functionAssertions.assertInvalidFunction(projection, INVALID_FUNCTION_ARGUMENT, message);
    }

    protected void assertInvalidFunction(@Language("SQL") String projection, ErrorCodeSupplier expectedErrorCode)
    {
        functionAssertions.assertInvalidFunction(projection, expectedErrorCode);
    }

    protected void assertFunctionThrowsIncorrectly(@Language("SQL") String projection, Class<? extends Throwable> throwableClass, @Language("RegExp") String message)
    {
        functionAssertions.assertFunctionThrowsIncorrectly(projection, throwableClass, message);
    }

    protected void assertNumericOverflow(String projection, String message)
    {
        functionAssertions.assertNumericOverflow(projection, message);
    }

    protected void assertInvalidCast(String projection)
    {
        functionAssertions.assertInvalidCast(projection);
    }

    protected void assertInvalidCast(@Language("SQL") String projection, String message)
    {
        functionAssertions.assertInvalidCast(projection, message);
    }

    protected void assertCachedInstanceHasBoundedRetainedSize(String projection)
    {
        functionAssertions.assertCachedInstanceHasBoundedRetainedSize(projection);
    }

    protected void assertNotSupported(String projection, String message)
    {
        assertTrinoExceptionThrownBy(() -> functionAssertions.executeProjectionWithFullEngine(projection))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage(message);
    }

    protected void tryEvaluateWithAll(String projection, Type expectedType)
    {
        functionAssertions.tryEvaluateWithAll(projection, expectedType);
    }

    protected void registerScalarFunction(SqlScalarFunction sqlScalarFunction)
    {
        functionAssertions.addFunctions(ImmutableList.of(sqlScalarFunction));
    }

    protected void registerScalar(Class<?> clazz)
    {
        functionAssertions.addFunctions(new FunctionListBuilder()
                .scalars(clazz)
                .getFunctions());
    }

    protected void registerParametricScalar(Class<?> clazz)
    {
        functionAssertions.addFunctions(new FunctionListBuilder()
                .scalar(clazz)
                .getFunctions());
    }

    protected void installPlugin(Plugin plugin)
    {
        functionAssertions.installPlugin(plugin);
    }

    protected static SqlTimestamp timestamp(int precision, String timestampValue)
    {
        LongTimestamp longTimestamp = castToLongTimestamp(precision, timestampValue);
        return SqlTimestamp.newInstance(precision, longTimestamp.getEpochMicros(), longTimestamp.getPicosOfMicro());
    }

    // this help function should only be used when the map contains null value
    // otherwise, use ImmutableMap.of()
    protected static <K, V> Map<K, V> asMap(List<K> keyList, List<V> valueList)
    {
        if (keyList.size() != valueList.size()) {
            fail("keyList should have same size with valueList");
        }
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            if (map.put(keyList.get(i), valueList.get(i)) != null) {
                fail("keyList should have same size with valueList");
            }
        }
        return map;
    }
}
