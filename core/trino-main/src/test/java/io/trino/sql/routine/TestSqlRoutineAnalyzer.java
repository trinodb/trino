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
package io.trino.sql.routine;

import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.MISSING_RETURN;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;

class TestSqlRoutineAnalyzer
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    void testParameters()
    {
        assertFails("FUNCTION test(x) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:15: Function parameters must have a name");

        assertFails("FUNCTION test(x int, y int, x bigint) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:29: Duplicate function parameter name: x");
    }

    @Test
    void testCharacteristics()
    {
        assertFails("FUNCTION test() RETURNS int CALLED ON NULL INPUT CALLED ON NULL INPUT RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:1: Multiple null-call clauses specified");

        assertFails("FUNCTION test() RETURNS int RETURNS NULL ON NULL INPUT CALLED ON NULL INPUT RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:1: Multiple null-call clauses specified");

        assertFails("FUNCTION test() RETURNS int COMMENT 'abc' COMMENT 'xyz' RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:1: Multiple comment clauses specified");

        assertFails("FUNCTION test() RETURNS int LANGUAGE abc LANGUAGE xyz RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:1: Multiple language clauses specified");

        assertFails("FUNCTION test() RETURNS int NOT DETERMINISTIC DETERMINISTIC RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:1: Multiple deterministic clauses specified");
    }

    @Test
    void testParameterTypeUnknown()
    {
        assertFails("FUNCTION test(x abc) RETURNS int RETURN 123")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:15: Unknown type: abc");
    }

    @Test
    void testReturnTypeUnknown()
    {
        assertFails("FUNCTION test() RETURNS abc RETURN 123")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:17: Unknown type: abc");
    }

    @Test
    void testReturnType()
    {
        analyze("FUNCTION test() RETURNS bigint RETURN smallint '123'");
        analyze("FUNCTION test() RETURNS varchar(10) RETURN 'test'");

        assertFails("FUNCTION test() RETURNS varchar(2) RETURN 'test'")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:43: Value of RETURN must evaluate to varchar(2) (actual: varchar(4))");

        assertFails("FUNCTION test() RETURNS bigint RETURN random()")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:39: Value of RETURN must evaluate to bigint (actual: double)");

        assertFails("FUNCTION test() RETURNS real RETURN random()")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:37: Value of RETURN must evaluate to real (actual: double)");
    }

    @Test
    void testLanguage()
    {
        assertThat(analyze("FUNCTION test() RETURNS bigint LANGUAGE SQL RETURN abs(-42)"))
                .returns(true, from(SqlRoutineAnalysis::deterministic));

        assertFails("FUNCTION test() RETURNS bigint LANGUAGE JAVASCRIPT RETURN abs(-42)")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Unsupported language: JAVASCRIPT");
    }

    @Test
    void testDeterministic()
    {
        assertThat(analyze("FUNCTION test() RETURNS bigint RETURN abs(-42)"))
                .returns(true, from(SqlRoutineAnalysis::deterministic));

        assertThat(analyze("FUNCTION test() RETURNS bigint DETERMINISTIC RETURN abs(-42)"))
                .returns(true, from(SqlRoutineAnalysis::deterministic));

        assertFails("FUNCTION test() RETURNS bigint NOT DETERMINISTIC RETURN abs(-42)")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:1: Deterministic function declared NOT DETERMINISTIC");

        assertThat(analyze("FUNCTION test() RETURNS varchar RETURN reverse('test')"))
                .returns(true, from(SqlRoutineAnalysis::deterministic));

        assertThat(analyze("FUNCTION test() RETURNS double NOT DETERMINISTIC RETURN 42 * random()"))
                .returns(false, from(SqlRoutineAnalysis::deterministic));

        assertFails("FUNCTION test() RETURNS double RETURN 42 * random()")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:1: Non-deterministic function declared DETERMINISTIC");
    }

    @Test
    void testIfConditionType()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  IF random() THEN
                    RETURN 13;
                  END IF;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 3:6: Condition of IF statement must evaluate to boolean (actual: double)");
    }

    @Test
    void testElseIfConditionType()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  IF false THEN
                    RETURN 13;
                  ELSEIF random() THEN
                    RETURN 13;
                  END IF;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 5:10: Condition of ELSEIF clause must evaluate to boolean (actual: double)");
    }

    @Test
    void testCaseWhenClauseValueType()
    {
        assertFails("""
                FUNCTION test(x int) RETURNS int
                BEGIN
                  CASE x
                    WHEN 13 THEN RETURN 13;
                    WHEN 'abc' THEN RETURN 42;
                  END CASE;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 5:10: WHEN clause value must evaluate to CASE value type integer (actual: varchar(3))");
    }

    @Test
    void testCaseWhenClauseConditionType()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  CASE
                    WHEN true THEN RETURN 42;
                    WHEN 13 THEN RETURN 13;
                  END CASE;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 5:10: Condition of WHEN clause must evaluate to boolean (actual: integer)");
    }

    @Test
    void testMissingReturn()
    {
        assertFails("FUNCTION test() RETURNS int BEGIN END")
                .hasErrorCode(MISSING_RETURN)
                .hasMessage("line 1:29: Function must end in a RETURN statement");

        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  IF false THEN
                    RETURN 13;
                  END IF;
                END
                """)
                .hasErrorCode(MISSING_RETURN)
                .hasMessage("line 2:1: Function must end in a RETURN statement");
    }

    @Test
    void testBadVariableDefault()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x int DEFAULT 'abc';
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 3:25: Value of DEFAULT must evaluate to integer (actual: varchar(3))");
    }

    @Test
    void testVariableAlreadyDeclared()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x int;
                  DECLARE x int;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 4:11: Variable already declared in this scope: x");

        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x int;
                  DECLARE y, x int;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 4:14: Variable already declared in this scope: x");

        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x, y, x int;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 3:17: Variable already declared in this scope: x");

        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x int;
                  BEGIN
                    DECLARE x int;
                  END;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 5:13: Variable already declared in this scope: x");

        analyze("""
                FUNCTION test() RETURNS int
                BEGIN
                  BEGIN
                    DECLARE x int;
                  END;
                  BEGIN
                    DECLARE x varchar;
                  END;
                  RETURN 0;
                END
                """);
    }

    @Test
    void testAssignmentUnknownTarget()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  SET x = 13;
                  RETURN 0;
                END
                """)
                .hasErrorCode(NOT_FOUND)
                .hasMessage("line 3:7: Variable cannot be resolved: x");
    }

    @Test
    void testAssignmentType()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  DECLARE x int;
                  SET x = 'abc';
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 4:11: Value of SET 'x' must evaluate to integer (actual: varchar(3))");
    }

    @Test
    void testWhileConditionType()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  WHILE 13 DO
                    RETURN 0;
                  END WHILE;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 3:9: Condition of WHILE statement must evaluate to boolean (actual: integer)");
    }

    @Test
    void testUntilConditionType()
    {
         assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  REPEAT
                    RETURN 42;
                  UNTIL 13 END REPEAT;
                  RETURN 0;
                END
                """)
                .hasErrorCode(TYPE_MISMATCH)
                 .hasMessage("line 5:9: Condition of REPEAT statement must evaluate to boolean (actual: integer)");
    }

    @Test
    void testIterateUnknownLabel()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  WHILE true DO
                    ITERATE abc;
                  END WHILE;
                  RETURN 0;
                END
                """)
                .hasErrorCode(NOT_FOUND)
                .hasMessage("line 4:13: Label not defined: abc");
    }

    @Test
    void testLeaveUnknownLabel()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  LEAVE abc;
                  RETURN 0;
                END
                """)
                .hasErrorCode(NOT_FOUND)
                .hasMessage("line 3:9: Label not defined: abc");
    }

    @Test
    void testDuplicateWhileLabel()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: WHILE true DO
                    LEAVE abc;
                    abc: WHILE true DO
                      LEAVE abc;
                    END WHILE;
                  END WHILE;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 5:5: Label already declared in this scope: abc");

        analyze("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: WHILE true DO
                    LEAVE abc;
                  END WHILE;
                  abc: WHILE true DO
                    LEAVE abc;
                  END WHILE;
                  RETURN 0;
                END
                """);
    }

    @Test
    void testDuplicateRepeatLabel()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: REPEAT
                    LEAVE abc;
                    abc: REPEAT
                      LEAVE abc;
                    UNTIL true END REPEAT;
                  UNTIL true END REPEAT;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 5:5: Label already declared in this scope: abc");

        analyze("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: REPEAT
                    LEAVE abc;
                  UNTIL true END REPEAT;
                  abc: REPEAT
                    LEAVE abc;
                  UNTIL true END REPEAT;
                  RETURN 0;
                END
                """);
    }

    @Test
    void testDuplicateLoopLabel()
    {
        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: LOOP
                    LEAVE abc;
                    abc: LOOP
                      LEAVE abc;
                    END LOOP;
                  END LOOP;
                  RETURN 0;
                END
                """)
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("line 5:5: Label already declared in this scope: abc");

        analyze("""
                FUNCTION test() RETURNS int
                BEGIN
                  abc: LOOP
                    LEAVE abc;
                  END LOOP;
                  abc: LOOP
                    LEAVE abc;
                  END LOOP;
                  RETURN 0;
                END
                """);
    }

    @Test
    void testSubquery()
    {
        assertFails("FUNCTION test() RETURNS int RETURN (SELECT 123)")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:36: Queries are not allowed in functions");

        assertFails("""
                FUNCTION test() RETURNS int
                BEGIN
                  RETURN (SELECT 123);
                END
                """)
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 3:10: Queries are not allowed in functions");
    }

    private static TrinoExceptionAssert assertFails(@Language("SQL") String function)
    {
        return assertTrinoExceptionThrownBy(() -> analyze(function));
    }

    private static SqlRoutineAnalysis analyze(@Language("SQL") String function)
    {
        FunctionSpecification specification = SQL_PARSER.createFunctionSpecification(function);

        TransactionManager transactionManager = new TestingTransactionManager();
        PlannerContext plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .build();
        return transaction(transactionManager, plannerContext.getMetadata(), new AllowAllAccessControl())
                .singleStatement()
                .execute(testSession(), transactionSession -> {
                    SqlRoutineAnalyzer analyzer = new SqlRoutineAnalyzer(plannerContext, WarningCollector.NOOP);
                    return analyzer.analyze(transactionSession, new AllowAllAccessControl(), specification);
                });
    }
}
