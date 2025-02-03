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
package io.trino.plugin.functions.python;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPythonFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        runner.installPlugin(new PythonFunctionsPlugin());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testInlineFunctions()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'twice')
                AS $$
                def twice(x):
                    return x * 2
                $$
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 21
                """))
                .matches("VALUES bigint '42'");
    }

    @Test
    public void testStripIndent()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    LANGUAGE PYTHON
                    WITH (handler = 'twice')
                    AS $$
                    def twice(x):
                        return x * 2
                    $$
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 21
                """))
                .matches("VALUES bigint '42'");
    }

    @Test
    public void testInvalidHandler()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'bad')
                AS $$
                def twice(x):
                    return x * 2
                $$
                SELECT my_func(13)
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage(
                        """
                        line 1:6: Invalid function 'my_func': Python error:
                        AttributeError: module 'guest' has no attribute 'bad'
                        Cannot find function 'bad' in 'guest'
                        """.stripTrailing());
    }

    @Test
    public void testSyntaxError()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'twice')
                AS $$
                defxxx twice(x):
                    return x * 2
                $$
                SELECT my_func(13)
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage(
                        """
                        line 1:6: Invalid function 'my_func': Python error:
                        File "/guest/guest.py", line 1
                            defxxx twice(x):
                                   ^^^^^
                        SyntaxError: invalid syntax
                        Failed to load Python module 'guest'
                        """.stripTrailing());
    }

    @Test
    public void testDivideByZero()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'divzero')
                AS $$
                def divzero(x):
                    return x / 0
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(DIVISION_BY_ZERO)
                .hasMessage("division by zero")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 2, in divzero
                            return x / 0
                                   ~~^~~
                        ZeroDivisionError: division by zero
                        """.stripTrailing());
    }

    @Test
    public void testNotSupported()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'notsupported')
                AS $$
                from trino import *
                def notsupported(x):
                    raise TrinoError(NOT_SUPPORTED, "test not-supported")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("test not-supported")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 3, in notsupported
                            raise TrinoError(NOT_SUPPORTED, "test not-supported")
                        trino.TrinoError: test not-supported
                        """.stripTrailing());
    }

    @Test
    public void testNumericValueOutOfRange()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'outofrange')
                AS $$
                from trino import *
                def outofrange(x):
                    raise NumericValueOutOfRangeError("test out-of-range")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("test out-of-range")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 3, in outofrange
                            raise NumericValueOutOfRangeError("test out-of-range")
                        trino.NumericValueOutOfRangeError: test out-of-range
                        """.stripTrailing());
    }

    @Test
    public void testInvalidFunctionArgument()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'badArg')
                AS $$
                from trino import *
                def badArg(x):
                    raise InvalidFunctionArgumentError("test bad-arg")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("test bad-arg")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 3, in badArg
                            raise InvalidFunctionArgumentError("test bad-arg")
                        trino.InvalidFunctionArgumentError: test bad-arg
                        """.stripTrailing());
    }

    @Test
    public void testGenericException()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'other')
                AS $$
                from trino import *
                def other(x):
                    raise ValueError("test other")
                $$
                SELECT my_func(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("ValueError: test other")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 3, in other
                            raise ValueError("test other")
                        ValueError: test other
                        """.stripTrailing());
    }

    @Test
    public void testTooFewArguments()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(s varchar)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'oops')
                AS $$
                def oops(a, b):
                    return a + b
                $$
                SELECT my_func(comment)
                FROM nation
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("TypeError: oops() missing 1 required positional argument: 'b'");
    }

    @Test
    public void testNoArgument()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION the_answer()
                RETURNS integer
                LANGUAGE PYTHON
                WITH (handler = 'answer')
                AS $$
                def answer():
                    return 42
                $$
                SELECT the_answer()
                """))
                .matches("VALUES 42");
    }

    @Test
    public void testMemoryLimitNoTraceback()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION huge(n bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'huge')
                AS $$
                def huge(n):
                    x = []
                    for i in range(0, 40):
                        x.append(bytearray(1024 * 1024))
                $$
                SELECT huge(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT)
                .hasMessage("Python MemoryError (no traceback available)");
    }

    @Test
    public void testMemoryLimitWithTraceback()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION huge(n bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'huge')
                AS $$
                def huge(n):
                    x = []
                    for i in range(0, 40):
                        x.append(bytearray(1024 * 1024 * 3))
                $$
                SELECT huge(nationkey)
                FROM nation
                """))
                .failure()
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT)
                .hasMessage("Python MemoryError")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 4, in huge
                            x.append(bytearray(1024 * 1024 * 3))
                                     ~~~~~~~~~^^^^^^^^^^^^^^^^^
                        MemoryError
                        """.stripTrailing());
    }

    @Test
    public void testFileSystemBadFileWrite()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION bad_write()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'bad_write')
                AS $$
                def bad_write():
                    with open('/test.txt', 'w') as f:
                        f.write('hello')
                $$
                SELECT bad_write()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("FileNotFoundError: [Errno 44] No such file or directory: '/test.txt'")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 2, in bad_write
                            with open('/test.txt', 'w') as f:
                                 ~~~~^^^^^^^^^^^^^^^^^^
                        FileNotFoundError: [Errno 44] No such file or directory: '/test.txt'
                        """.stripTrailing());
    }

    @Test
    public void testFileSystemOverwritePython()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION bad_write()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'bad_write')
                AS $$
                import os, sysconfig
                libdir = sysconfig.get_path('purelib')
                def bad_write():
                    name = os.path.join(libdir, 'trino.py')
                    assert os.path.exists(name)
                    with open(name, 'w') as f:
                        f.write('hello')
                $$
                SELECT bad_write()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("OSError: [Errno 58] Not supported")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        OSError: [Errno 58] Not supported

                        During handling of the above exception, another exception occurred:

                        Traceback (most recent call last):
                          File "/guest/guest.py", line 6, in bad_write
                            with open(name, 'w') as f:
                                 ~~~~^^^^^^^^^^^
                        OSError: [Errno 58] Not supported
                        """.stripTrailing());
    }

    @Test
    public void testFileSystemSmallFileWrite()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION small_write()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'small_write')
                AS $$
                def small_write():
                    with open('/guest/test.txt', 'w') as f:
                        f.write('hello')
                    with open('/guest/test.txt', 'r') as f:
                        return f.read()
                $$
                SELECT small_write()
                """))
                .matches("SELECT varchar 'hello'");
    }

    @Test
    public void testFileSystemLargeFileWrite()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION large_write()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'large_write')
                AS $$
                def large_write():
                    file_size = 16 * 1024 * 1024
                    data = b'x' * 4096
                    with open('/guest/test.zero', 'wb') as f:
                        for _ in range(file_size // len(data)):
                            f.write(data)
                $$
                SELECT large_write()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("OSError: [Errno 29] I/O error")
                .hasRootCauseMessage(
                        """
                        Python traceback:
                        Traceback (most recent call last):
                          File "/guest/guest.py", line 6, in large_write
                            f.write(data)
                            ~~~~~~~^^^^^^
                        OSError: [Errno 29] I/O error

                        During handling of the above exception, another exception occurred:

                        Traceback (most recent call last):
                          File "/guest/guest.py", line 4, in large_write
                            with open('/guest/test.zero', 'wb') as f:
                                 ~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^
                        OSError: [Errno 29] I/O error
                        """.stripTrailing());
    }

    @Test
    public void testSplitWords()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION reverse_words(s varchar)
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'reverse_words')
                AS $$
                import re

                def reverse(s):
                    str = ""
                    for i in s:
                        str = i + str
                    return str

                pattern = re.compile(r"\\w+[.,'!?\\"]\\w*")

                def process_word(word):
                    # Reverse only words without non-letter signs
                    return word if pattern.match(word) else reverse(word)

                def reverse_words(payload):
                    text_words = payload.split(' ')
                    return ' '.join([process_word(w) for w in text_words])
                $$
                SELECT comment, reverse_words(comment)
                FROM nation
                WHERE nationkey IN (5, 6, 12)
                """))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('ven packages wake quickly. regu', 'nev segakcap ekaw quickly. uger'),
                            ('refully final requests. regular, ironi', 'yllufer lanif requests. regular, inori'),
                            ('ously. final, express gifts cajole a', 'ously. final, sserpxe stfig elojac a')
                        """);
    }

    @Test
    public void testAssert()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION test_assert()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'test_assert')
                AS $$
                def test_assert():
                    assert False, "test fail"
                $$
                SELECT test_assert()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("AssertionError: test fail");
    }

    @Test
    public void testNulls()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION null_input(a varchar, b bigint)
                RETURNS boolean
                LANGUAGE PYTHON
                WITH (handler = 'null_input')
                AS $$
                def null_input(a, b):
                    assert a is None
                    assert b is None
                    return True
                $$
                SELECT null_input(null, null)
                """))
                .matches("VALUES true");

        assertThat(assertions.query(
                """
                WITH FUNCTION null_output()
                RETURNS boolean
                LANGUAGE PYTHON
                WITH (handler = 'null_output')
                AS $$
                def null_output():
                    return None
                $$
                SELECT null_output()
                """))
                .matches("VALUES cast(null AS boolean)");
    }

    @Test
    public void testUnupportedArgumentType()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION invalid(x HyperLogLog)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return None
                $$
                SELECT invalid()
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:6: Invalid function 'invalid': Unsupported type: HyperLogLog");
    }

    @Test
    public void testUnsupportedReturnType()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION invalid()
                RETURNS HyperLogLog
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return None
                $$
                SELECT invalid()
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:6: Invalid function 'invalid': Unsupported type: HyperLogLog");
    }

    @Test
    public void testTypeBoolean()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION xor(a boolean, b boolean)
                RETURNS boolean
                LANGUAGE PYTHON
                WITH (handler = 'xor')
                AS $$
                import operator
                def xor(a, b):
                    return operator.xor(a, b)
                $$
                SELECT xor(false, false), xor(false, true), xor(true, false), xor(true, true)
                """))
                .matches("VALUES (false, true, true, false)");
    }

    @Test
    public void testTypeBigint()
    {
        String query =
                """
                WITH FUNCTION multiply(x bigint, y bigint)
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                """;

        assertThat(assertions.query(
                query + "SELECT multiply(12345678, 87654321)"))
                .matches("VALUES 1082152022374638");

        assertThat(assertions.query(
                query + "SELECT multiply(12345678901, 10987654321)"))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range for BIGINT");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_bigint_return()
                RETURNS bigint
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 'hello'
                $$
                SELECT bad_bigint_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type BIGINT: " +
                        "TypeError: 'str' object cannot be interpreted as an integer");
    }

    @Test
    public void testTypeInteger()
    {
        String query =
                """
                WITH FUNCTION multiply(x integer, y integer)
                RETURNS integer
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                """;

        assertThat(assertions.query(
                query + "SELECT multiply(12345, 54321)"))
                .matches("VALUES 670592745");

        assertThat(assertions.query(
                query + "SELECT multiply(12345678, 87654321)"))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range for INTEGER");
    }

    @Test
    public void testTypeSmallint()
    {
        String query =
                """
                WITH FUNCTION multiply(x smallint, y smallint)
                RETURNS smallint
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                """;

        assertThat(assertions.query(
                query + "SELECT multiply(smallint '123', smallint '231')"))
                .matches("VALUES smallint '28413'");

        assertThat(assertions.query(
                query + "SELECT multiply(smallint '12345', smallint '32145')"))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range for SMALLINT");
    }

    @Test
    public void testTypeTinyint()
    {
        String query =
                """
                WITH FUNCTION multiply(x tinyint, y tinyint)
                RETURNS tinyint
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                """;

        assertThat(assertions.query(
                query + "SELECT multiply(tinyint '9', tinyint '13')"))
                .matches("VALUES tinyint '117'");

        assertThat(assertions.query(
                query + "SELECT multiply(tinyint '123', tinyint '99')"))
                .failure()
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range for TINYINT");
    }

    @Test
    public void testTypeDouble()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION multiply(x double, y double)
                RETURNS double
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                SELECT multiply(123.45, 67.89)
                """))
                .matches("VALUES double '8381.0205'");
    }

    @Test
    public void testTypeReal()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION multiply(x real, y real)
                RETURNS real
                LANGUAGE PYTHON
                WITH (handler = 'multiply')
                AS $$
                def multiply(x, y):
                    return x * y
                $$
                SELECT multiply(123.45, 67.89)
                """))
                .matches("VALUES real '8381.0205'");
    }

    @Test
    public void testTypeDecimal()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION test_decimal_short(x decimal(18, 5))
                RETURNS decimal(18, 5)
                LANGUAGE PYTHON
                WITH (handler = 'square')
                AS $$
                def square(x):
                    assert str(x) == '123.45600'
                    return x * x
                $$
                SELECT test_decimal_short(123.456)
                """))
                .matches("VALUES cast(15241.38394 AS decimal(18, 5))");

        assertThat(assertions.query(
                """
                WITH FUNCTION test_decimal_long(x decimal(38, 5))
                RETURNS decimal(38, 5)
                LANGUAGE PYTHON
                WITH (handler = 'test')
                AS $$
                from decimal import Decimal
                def test(x):
                    assert str(x) == '12345678901234567890.12340'
                    return x * Decimal('123.456')
                $$
                SELECT test_decimal_long(12345678901234567890.1234)
                """))
                .matches("VALUES cast(1524148134430814813443.07447 AS decimal(38, 5))");
    }

    @Test
    public void testTypeVarchar()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_concat(x varchar(16), y varchar(16))
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'concat')
                AS $$
                def concat(x, y):
                    return x + y
                $$
                SELECT my_concat(cast('hello' AS varchar(8)), cast('world' AS varchar(8)))
                """))
                .matches("VALUES cast('helloworld' AS varchar)");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_varchar_return()
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_varchar_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type VARCHAR: " +
                        "TypeError: expected an instance of type 'str'");

        assertThat(assertions.query(
                """
                WITH FUNCTION my_concat(x varchar, y varchar)
                RETURNS varchar(32)
                LANGUAGE PYTHON
                WITH (handler = 'concat')
                AS $$
                def concat(x, y):
                    return x + y
                $$
                SELECT my_concat('hello', 'world')
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:6: Invalid function 'my_concat': Type VARCHAR(x) not supported as return type. Use VARCHAR instead.");
    }

    @Test
    public void testTypeVarbinary()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_concat(x varbinary, y varbinary)
                RETURNS varbinary
                LANGUAGE PYTHON
                WITH (handler = 'concat')
                AS $$
                def concat(x, y):
                    return bytearray(x + y)
                $$
                SELECT my_concat(varbinary 'abc', varbinary 'xyz')
                """))
                .matches("VALUES varbinary 'abcxyz'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_varbinary_return()
                RETURNS varbinary
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 'hello'
                $$
                SELECT bad_varbinary_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type VARBINARY: " +
                        "TypeError: a bytes-like object is required, not 'str'");
    }

    @Test
    public void testTypeDate()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION add_week(x date)
                RETURNS date
                LANGUAGE PYTHON
                WITH (handler = 'add_week')
                AS $$
                from datetime import datetime, timedelta
                def add_week(x):
                    assert str(x) == '2024-06-27'
                    return x + timedelta(weeks=1)
                $$
                SELECT add_week(date '2024-06-27')
                """))
                .matches("VALUES date '2024-07-04'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_date_return()
                RETURNS date
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_date_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type DATE: " +
                        "TypeError: expected an instance of type 'datetime.date'");
    }

    @Test
    public void testTypeTime()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(6))
                RETURNS time(6)
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                from datetime import time
                def get_time(x):
                    assert str(x) == '03:23:56.123456'
                    r = time(11, 42, 59, 246912)
                    assert str(r) == '11:42:59.246912'
                    return r
                $$
                SELECT get_time(time '3:23:56.123456')
                """))
                .matches("VALUES time '11:42:59.246912'");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(6))
                RETURNS time(3)
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                def get_time(x):
                    assert str(x) == '23:42:59.999666'
                    return x
                $$
                SELECT get_time(time '23:42:59.999666')
                """))
                .matches("VALUES time '23:43:00.000'");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(9))
                RETURNS time(6)
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                def get_time(x):
                    assert str(x) == '23:42:59.999556'
                    return x
                $$
                SELECT get_time(time '23:42:59.999555888')
                """))
                .matches("VALUES time '23:42:59.999556'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_time_return()
                RETURNS time
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_time_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type TIME: " +
                        "TypeError: expected an instance of type 'datetime.time'");
    }

    @Test
    public void testTypeTimeWithTimeZone()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(6) with time zone)
                RETURNS time(6) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                from datetime import time, timezone, timedelta
                def get_time(x):
                    assert str(x) == '03:23:56.123456-08:15'
                    r = time(11, 42, 59, 246912, timezone(timedelta(minutes=155)))
                    assert str(r) == '11:42:59.246912+02:35'
                    return r
                $$
                SELECT get_time(cast('3:23:56.123456-08:15' AS time(6) with time zone))
                """))
                .matches("VALUES cast('11:42:59.246912+02:35' AS time(6) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(6) with time zone)
                RETURNS time(3) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                def get_time(x):
                    assert str(x) == '23:42:59.999666+11:45'
                    return x
                $$
                SELECT get_time(cast('23:42:59.999666+11:45' AS time(6) with time zone))
                """))
                .matches("VALUES cast('23:43:00.000+11:45' AS time(3) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(9) with time zone)
                RETURNS time(6) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                def get_time(x):
                    assert str(x) == '23:42:59.999556-08:00'
                    return x
                $$
                SELECT get_time(cast('23:42:59.999555888-08:00' AS time(9) with time zone))
                """))
                .matches("VALUES cast('23:42:59.999556-08:00' AS time(6) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_time(x time(12) with time zone)
                RETURNS time(12) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                def get_time(x):
                    assert str(x) == '23:42:59.999556-10:00'
                    return x
                $$
                SELECT get_time(cast('23:42:59.999555888555-10:00' AS time(12) with time zone))
                """))
                .matches("VALUES cast('23:42:59.999556-10:00' AS time(12) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_time_return()
                RETURNS time with time zone
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_time_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type TIME WITH TIME ZONE: " +
                        "TypeError: expected an instance of type 'datetime.time'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_time_no_tz()
                RETURNS time with time zone
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                from datetime import time
                def invalid():
                    return time(1, 22, 33)
                $$
                SELECT bad_time_no_tz()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'datetime.time' to Trino type TIME WITH TIME ZONE: " +
                        "ValueError: time instance does not have tzinfo");
    }

    @Test
    public void testTypeTimestamp()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION add_timestamp(x timestamp(6))
                RETURNS timestamp(6)
                LANGUAGE PYTHON
                WITH (handler = 'add_timestamp')
                AS $$
                from datetime import timedelta
                def add_timestamp(x):
                    assert str(x) == '2024-05-06 11:42:54.123456'
                    r = x + timedelta(weeks=5, days=2, hours=3, minutes=4, seconds=5, microseconds=123456)
                    assert str(r) == '2024-06-12 14:46:59.246912'
                    return r
                $$
                SELECT add_timestamp(timestamp '2024-05-06 11:42:54.123456')
                """))
                .matches("VALUES timestamp '2024-06-12 14:46:59.246912'");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_timestamp(x timestamp(9))
                RETURNS timestamp(6)
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.123456'
                    return x
                $$
                SELECT get_timestamp(timestamp '2024-11-12 23:42:59.123456123')
                """))
                .matches("VALUES timestamp '2024-11-12 23:42:59.123456'");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_short_timestamp(x timestamp(12))
                RETURNS timestamp(3)
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.999667'
                    return x
                $$
                SELECT to_short_timestamp(timestamp '2024-11-12 23:42:59.999666555444')
                """))
                .matches("VALUES timestamp '2024-11-12 23:43:00.000'");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_long_timestamp(x timestamp(6))
                RETURNS timestamp(9)
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.999666'
                    return x
                $$
                SELECT to_long_timestamp(timestamp '2024-11-12 23:42:59.999666')
                """))
                .matches("VALUES timestamp '2024-11-12 23:42:59.999666000'");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_long_timestamp(x timestamp(6))
                RETURNS timestamp(12)
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.999666'
                    return x
                $$
                SELECT to_long_timestamp(timestamp '2024-11-12 23:42:59.999666')
                """))
                .matches("VALUES timestamp '2024-11-12 23:42:59.999666000000'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_timestamp_return()
                RETURNS timestamp
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_timestamp_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type TIMESTAMP: " +
                        "TypeError: expected an instance of type 'datetime.datetime'");
    }

    @Test
    public void testTypeTimestampWithTimeZone()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_timestamp(x timestamp(6) with time zone)
                RETURNS timestamp(6) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_time')
                AS $$
                from datetime import datetime, timezone, timedelta
                def get_time(x):
                    assert str(x) == '2024-05-06 03:23:56.123456-08:15'
                    r = datetime(2024, 8, 17, 11, 42, 59, 246912, timezone(timedelta(minutes=155)))
                    assert str(r) == '2024-08-17 11:42:59.246912+02:35'
                    return r
                $$
                SELECT get_timestamp(cast('2024-05-06 3:23:56.123456-08:15' AS timestamp(6) with time zone))
                """))
                .matches("VALUES cast('2024-08-17 11:42:59.246912+02:35' AS timestamp(6) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_short_timestamp(x timestamp(9) with time zone)
                RETURNS timestamp(2) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.888889+11:45'
                    return x
                $$
                SELECT to_short_timestamp(cast('2024-11-12 23:42:59.888888888+11:45' AS timestamp(9) with time zone))
                """))
                .matches("VALUES cast('2024-11-12 23:42:59.89+11:45' AS timestamp(2) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_long_timestamp(x timestamp(3) with time zone)
                RETURNS timestamp(6) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.888000-08:00'
                    return x
                $$
                SELECT to_long_timestamp(cast('2024-11-12 23:42:59.888-08:00' AS timestamp(3) with time zone))
                """))
                .matches("VALUES cast('2024-11-12 23:42:59.888000-08:00' AS timestamp(6) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION to_long_timestamp(x timestamp(6) with time zone)
                RETURNS timestamp(12) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-11-12 23:42:59.999666-10:00'
                    return x
                $$
                SELECT to_long_timestamp(cast('2024-11-12 23:42:59.999666-10:00' AS timestamp(6) with time zone))
                """))
                .matches("VALUES cast('2024-11-12 23:42:59.999666000000-10:00' AS timestamp(12) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION from_non_fixed(x timestamp(6) with time zone)
                RETURNS timestamp(6) with time zone
                LANGUAGE PYTHON
                WITH (handler = 'get_timestamp')
                AS $$
                def get_timestamp(x):
                    assert str(x) == '2024-07-04 21:35:20.123456-07:00'
                    return x
                $$
                SELECT from_non_fixed(cast('2024-07-04 21:35:20.123456 America/Los_Angeles' AS timestamp(6) with time zone))
                """))
                .matches("VALUES cast('2024-07-04 21:35:20.123456-07:00' AS timestamp(6) with time zone)");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_timestamp_return()
                RETURNS timestamp with time zone
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_timestamp_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type TIMESTAMP WITH TIME ZONE: " +
                        "TypeError: expected an instance of type 'datetime.datetime'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_timestamp_no_tz()
                RETURNS timestamp with time zone
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                from datetime import datetime
                def invalid():
                    return datetime(2, 3, 4, 5, 6, 7)
                $$
                SELECT bad_timestamp_no_tz()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'datetime.datetime' to Trino type TIMESTAMP WITH TIME ZONE: " +
                        "ValueError: datetime instance does not have tzinfo");
    }

    @Test
    public void testTypeIntervalYearToMonth()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION add_months(x interval year to month)
                RETURNS interval year to month
                LANGUAGE PYTHON
                WITH (handler = 'add_months')
                AS $$
                def add_months(x):
                    return x + 42;
                $$
                SELECT add_months(interval '5-9' year to month)
                """))
                .matches("VALUES interval '9-3' year to month");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_interval_return()
                RETURNS interval year to month
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return '13-2'
                $$
                SELECT bad_interval_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type INTERVAL YEAR TO MONTH: " +
                        "TypeError: 'str' object cannot be interpreted as an integer");
    }

    @Test
    public void testTypeIntervalDayToSecond()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_interval(x interval day to second)
                RETURNS interval day to second
                LANGUAGE PYTHON
                WITH (handler = 'get_interval')
                AS $$
                from datetime import timedelta
                def get_interval(x):
                    assert str(x) == '5 days, 9:23:56.123000'
                    return timedelta(days=3, hours=18, minutes=42, seconds=33, microseconds=888888)
                $$
                SELECT get_interval(interval '5 9:23:56.123' day to second)
                """))
                .matches("VALUES (interval '3 18:42:33.889' day to second)");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_interval_return()
                RETURNS interval day to second
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_interval_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type INTERVAL DAY TO SECOND: " +
                        "TypeError: expected an instance of type 'datetime.timedelta'");
    }

    @Test
    public void testTypeJson()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION update_json(x json)
                RETURNS json
                LANGUAGE PYTHON
                WITH (handler = 'update_json')
                AS $$
                import json
                def update_json(x):
                    assert x == '{"bar":456,"foo":123}'
                    v = json.loads(x)
                    v['abc'] = 'xyz'
                    return json.dumps(v)
                $$
                SELECT update_json(json '{"foo": 123, "bar": 456}')
                """))
                .matches("""
                         VALUES json '{"abc": "xyz", "bar": 456, "foo": 123}'
                         """);

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_json_return()
                RETURNS json
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_json_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type JSON: " +
                        "TypeError: expected an instance of type 'str'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_json_value()
                RETURNS json
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 'xxx'
                $$
                SELECT bad_json_value()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Python function returned invalid JSON value");
    }

    @Test
    public void testTypeUuid()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION uuid_to_str(x uuid)
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'convert')
                AS $$
                def convert(x):
                    return str(x)
                $$
                SELECT uuid_to_str(uuid '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1'),
                       uuid_to_str(uuid 'dfa7eaf8-6a26-5749-8d36-336025df74e8')
                """))
                .skippingTypesCheck()
                .matches("""
                         VALUES ('6b5f5b65-67e4-43b0-8ee3-586cd49f58a1',
                                 'dfa7eaf8-6a26-5749-8d36-336025df74e8')
                         """);

        assertThat(assertions.query(
                """
                WITH FUNCTION str_to_uuid(x varchar)
                RETURNS uuid
                LANGUAGE PYTHON
                WITH (handler = 'convert')
                AS $$
                from uuid import UUID
                def convert(x):
                    return UUID(x)
                $$
                SELECT str_to_uuid('6b5f5b65-67e4-43b0-8ee3-586cd49f58a1'),
                       str_to_uuid('dfa7eaf8-6a26-5749-8d36-336025df74e8')
                """))
                .matches("""
                         VALUES (uuid '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1',
                                 uuid 'dfa7eaf8-6a26-5749-8d36-336025df74e8')
                         """);

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_uuid_return()
                RETURNS uuid
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 'hello'
                $$
                SELECT bad_uuid_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type UUID: " +
                        "TypeError: expected an instance of type 'UUID'");
    }

    @Test
    public void testTypeIpaddress()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION ip_to_str(x ipaddress)
                RETURNS varchar
                LANGUAGE PYTHON
                WITH (handler = 'convert')
                AS $$
                def convert(x):
                    return type(x).__name__ + ':' + str(x)
                $$
                SELECT ip_to_str(ipaddress '192.168.1.5'),
                       ip_to_str(ipaddress '12.34.56.78'),
                       ip_to_str(ipaddress '2001:0db8:0000:0000:0000:ff00:0042:8329'),
                       ip_to_str(ipaddress '2001:db8:0:0:1::1'),
                       ip_to_str(ipaddress '::ffff:1.2.3.4')
                """))
                .skippingTypesCheck()
                .matches("""
                         VALUES ('IPv4Address:192.168.1.5',
                                 'IPv4Address:12.34.56.78',
                                 'IPv6Address:2001:db8::ff00:42:8329',
                                 'IPv6Address:2001:db8::1:0:0:1',
                                 'IPv4Address:1.2.3.4')
                         """);

        assertThat(assertions.query(
                """
                WITH FUNCTION str_to_ip(x varchar)
                RETURNS ipaddress
                LANGUAGE PYTHON
                WITH (handler = 'convert')
                AS $$
                from ipaddress import ip_address
                def convert(x):
                    return ip_address(x)
                $$
                SELECT str_to_ip('192.168.1.5'),
                       str_to_ip('12.34.56.78'),
                       str_to_ip('2001:0db8:0000:0000:0000:ff00:0042:8329'),
                       str_to_ip('2001:db8:0:0:1::1'),
                       str_to_ip('::ffff:1.2.3.4')
                """))
                .matches("""
                         VALUES (ipaddress '192.168.1.5',
                                 ipaddress '12.34.56.78',
                                 ipaddress '2001:db8::ff00:42:8329',
                                 ipaddress '2001:db8::1:0:0:1',
                                 ipaddress '1.2.3.4')
                         """);

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_ip_return()
                RETURNS ipaddress
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 'hello'
                $$
                SELECT bad_ip_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type IPADDRESS: " +
                        "TypeError: expected an instance of type 'ipaddress.IPv4Address' or 'ipaddress.IPv6Address'");
    }

    @Test
    public void testTypeRow()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_row(x row(a integer, b varchar))
                RETURNS row(a integer, b varchar)
                LANGUAGE PYTHON
                WITH (handler = 'get_row')
                AS $$
                def get_row(x):
                    assert x == (123, 'hello')
                    return x
                $$
                SELECT get_row(row(123, 'hello'))
                """))
                .skippingTypesCheck()
                .matches("SELECT row(123, 'hello')");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_row_return()
                RETURNS row(a integer, b varchar)
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_row_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type ROW: " +
                        "TypeError: expected an instance of type 'tuple'");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_row_count()
                RETURNS row(a integer, b varchar)
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return (123, 'hello', 789)
                $$
                SELECT bad_row_count()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'tuple' to Trino type ROW: " +
                        "ValueError: tuple has 3 fields, expected 2 fields for row");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_row_value()
                RETURNS row(a integer, b varchar)
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return ('hello', 123)
                $$
                SELECT bad_row_value()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'str' to Trino type INTEGER: " +
                        "TypeError: 'str' object cannot be interpreted as an integer");
    }

    @Test
    public void testTypeArray()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_array(x array(integer))
                RETURNS array(integer)
                LANGUAGE PYTHON
                WITH (handler = 'get_array')
                AS $$
                def get_array(x):
                    assert x == [1, 2, 3]
                    return x
                $$
                SELECT get_array(array[1, 2, 3])
                """))
                .matches("VALUES array[1, 2, 3]");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_array_return()
                RETURNS array(integer)
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_array_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type ARRAY: " +
                        "TypeError: expected an instance of type 'list'");
    }

    @Test
    public void testTypeMap()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_map(x map(integer, varchar))
                RETURNS map(integer, varchar)
                LANGUAGE PYTHON
                WITH (handler = 'get_map')
                AS $$
                def get_map(x):
                    assert x == {1: 'a', 2: 'b', 3: 'c'}
                    return x
                $$
                SELECT get_map(map_from_entries(ARRAY[(1, 'a'), (2, 'b'), (3, 'c')]))
                """))
                .skippingTypesCheck()
                .matches("VALUES map_from_entries(ARRAY[(1, 'a'), (2, 'b'), (3, 'c')])");

        assertThat(assertions.query(
                """
                WITH FUNCTION bad_map_return()
                RETURNS map(integer, varchar)
                LANGUAGE PYTHON
                WITH (handler = 'invalid')
                AS $$
                def invalid():
                    return 123
                $$
                SELECT bad_map_return()
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Failed to convert Python result type 'int' to Trino type MAP: " +
                        "TypeError: expected an instance of type 'dict'");
    }

    @Test
    public void testNestedTypes()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION get_nested(x row(array(varchar), map(varchar, integer), row(integer, integer)))
                RETURNS row(array(varchar), map(varchar, integer), row(integer, integer))
                LANGUAGE PYTHON
                WITH (handler = 'get_nested')
                AS $$
                def get_nested(x):
                    assert x == (['a', 'b'], {'c': 1, 'd': 2}, (3, 4))
                    return x
                $$
                SELECT get_nested(row(array['a', 'b'], map_from_entries(ARRAY[('c', 1), ('d', 2)]), row(3, 4)))
                """))
                .skippingTypesCheck()
                .matches("SELECT row(array['a', 'b'], map_from_entries(ARRAY[('c', 1), ('d', 2)]), row(3, 4))");

        assertThat(assertions.query(
                """
                WITH FUNCTION get_nested(x row(
                    boolean,
                    boolean,
                    bigint,
                    integer,
                    smallint,
                    tinyint,
                    double,
                    real,
                    decimal(18, 5),
                    decimal(25, 5),
                    varchar,
                    varbinary,
                    date,
                    time(9),
                    time(9) with time zone,
                    time(12) with time zone,
                    timestamp(3),
                    timestamp(9),
                    timestamp(3) with time zone,
                    timestamp(9) with time zone,
                    interval year to month,
                    interval day to second,
                    json,
                    uuid,
                    ipaddress))
                RETURNS row(
                    boolean,
                    boolean,
                    bigint,
                    integer,
                    smallint,
                    tinyint,
                    double,
                    real,
                    decimal(18, 5),
                    decimal(25, 5),
                    varchar,
                    varbinary,
                    date,
                    time(5),
                    time(5) with time zone,
                    time(6) with time zone,
                    timestamp(3),
                    timestamp(5),
                    timestamp(3) with time zone,
                    timestamp(5) with time zone,
                    interval year to month,
                    interval day to second,
                    json,
                    uuid,
                    ipaddress)
                LANGUAGE PYTHON
                WITH (handler = 'get_nested')
                AS $$
                from decimal import Decimal
                from datetime import date, time, datetime, timedelta, timezone
                from uuid import UUID
                from ipaddress import ip_address
                def get_nested(x):
                    assert x == (
                        None,
                        True,
                        1234567890123456789,
                        1234567890,
                        12345,
                        123,
                        8381.0205,
                        123.5,
                        Decimal('123.45600'),
                        Decimal('12345678901234567890.12340'),
                        'hello',
                        b'world',
                        date(2024, 6, 27),
                        time(3, 23, 56, 123457),
                        time(3, 23, 56, 123457, timezone(timedelta(minutes=155))),
                        time(3, 23, 56, 123457, timezone(timedelta(minutes=155))),
                        datetime(2024, 5, 6, 11, 42, 54, 123000),
                        datetime(2024, 5, 6, 11, 42, 54, 123457),
                        datetime(2024, 5, 6, 11, 42, 54, 123000, timezone(timedelta(hours=-7))),
                        datetime(2024, 5, 6, 11, 42, 54, 123457, timezone(timedelta(hours=-7))),
                        67,
                        timedelta(days=5, hours=9, minutes=23, seconds=56, milliseconds=123),
                        '{"bar":456,"foo":123}',
                        UUID('6b5f5b65-67e4-43b0-8ee3-586cd49f58a1'),
                        ip_address('12.34.56.78'))
                    return x
                $$
                SELECT get_nested(row(
                    cast(null AS boolean),
                    true,
                    1234567890123456789,
                    1234567890,
                    smallint '12345',
                    tinyint '123',
                    double '8381.0205',
                    real '123.5',
                    cast(123.456 AS decimal(15, 5)),
                    cast(12345678901234567890.1234 AS decimal(25, 5)),
                    varchar 'hello',
                    varbinary 'world',
                    date '2024-06-27',
                    cast('3:23:56.123456888' AS time(9)),
                    cast('3:23:56.123456888+02:35' AS time(9) with time zone),
                    cast('3:23:56.123456888999+02:35' AS time(12) with time zone),
                    cast('2024-05-06 11:42:54.123' as timestamp(3)),
                    cast('2024-05-06 11:42:54.123456888' as timestamp(9)),
                    cast('2024-05-06 11:42:54.123 America/Los_Angeles' AS timestamp(3) with time zone),
                    cast('2024-05-06 11:42:54.123456888 America/Los_Angeles' AS timestamp(9) with time zone),
                    interval '5-7' year to month,
                    interval '5 9:23:56.123888' day to second,
                    json '{"foo": 123, "bar": 456}',
                    uuid '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1',
                    ipaddress '12.34.56.78'))
                """))
                .matches("""
                         SELECT row(
                             cast(null AS boolean),
                             true,
                             1234567890123456789,
                             1234567890,
                             smallint '12345',
                             tinyint '123',
                             double '8381.0205',
                             real '123.5',
                             cast(123.456 AS decimal(18, 5)),
                             cast(12345678901234567890.1234 AS decimal(25, 5)),
                             varchar 'hello',
                             varbinary 'world',
                             date '2024-06-27',
                             time '03:23:56.12346',
                             time '03:23:56.12346+02:35',
                             time '03:23:56.123457+02:35',
                             timestamp '2024-05-06 11:42:54.123',
                             timestamp '2024-05-06 11:42:54.12346',
                             timestamp '2024-05-06 11:42:54.123-07:00',
                             timestamp '2024-05-06 11:42:54.12346-07:00',
                             interval '5-7' year to month,
                             interval '5 09:23:56.123' day to second,
                             json '{"bar": 456, "foo": 123}',
                             uuid '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1',
                             ipaddress '12.34.56.78')
                         """);
    }
}
