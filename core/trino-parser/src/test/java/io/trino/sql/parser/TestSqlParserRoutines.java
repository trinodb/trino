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
package io.trino.sql.parser;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.AssignmentStatement;
import io.trino.sql.tree.CommentCharacteristic;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.CreateFunction;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DeterministicCharacteristic;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfStatement;
import io.trino.sql.tree.LanguageCharacteristic;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.ReturnStatement;
import io.trino.sql.tree.ReturnsClause;
import io.trino.sql.tree.SecurityCharacteristic;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhileStatement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.QueryUtil.functionCall;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.parser.ParserAssert.functionSpecification;
import static io.trino.sql.parser.ParserAssert.statement;
import static io.trino.sql.tree.NullInputCharacteristic.calledOnNullInput;
import static io.trino.sql.tree.NullInputCharacteristic.returnsNullOnNullInput;
import static io.trino.sql.tree.SecurityCharacteristic.Security.DEFINER;
import static io.trino.sql.tree.SecurityCharacteristic.Security.INVOKER;
import static org.assertj.core.api.Assertions.assertThat;

class TestSqlParserRoutines
{
    @Test
    public void testStandaloneFunction()
    {
        assertThat(functionSpecification("FUNCTION foo() RETURNS bigint RETURN 42"))
                .ignoringLocation()
                .isEqualTo(new FunctionSpecification(
                        location(),
                        QualifiedName.of("foo"),
                        ImmutableList.of(),
                        returns(type("bigint")),
                        ImmutableList.of(),
                        new ReturnStatement(location(), literal(42))));
    }

    @Test
    void testInlineFunction()
    {
        assertThat(statement("""
                WITH
                  FUNCTION answer()
                  RETURNS BIGINT
                  RETURN 42
                SELECT answer()
                """))
                .ignoringLocation()
                .isEqualTo(query(
                        new FunctionSpecification(
                                location(),
                                QualifiedName.of("answer"),
                                ImmutableList.of(),
                                returns(type("BIGINT")),
                                ImmutableList.of(),
                                new ReturnStatement(location(), literal(42))),
                        selectList(new FunctionCall(QualifiedName.of("answer"), ImmutableList.of()))));
    }

    @Test
    void testSimpleFunction()
    {
        assertThat(statement("""
                CREATE FUNCTION hello(s VARCHAR)
                RETURNS varchar
                LANGUAGE SQL
                DETERMINISTIC
                CALLED ON NULL INPUT
                SECURITY INVOKER
                COMMENT 'hello world function'
                RETURN CONCAT('Hello, ', s, '!')
                """))
                .ignoringLocation()
                .isEqualTo(new CreateFunction(
                        location(),
                        new FunctionSpecification(
                                location(),
                                QualifiedName.of("hello"),
                                ImmutableList.of(parameter("s", type("VARCHAR"))),
                                returns(type("varchar")),
                                ImmutableList.of(
                                        new LanguageCharacteristic(location(), identifier("SQL")),
                                        new DeterministicCharacteristic(location(), true),
                                        calledOnNullInput(),
                                        new SecurityCharacteristic(location(), INVOKER),
                                        new CommentCharacteristic(new NodeLocation(1, 1), "hello world function")),
                                new ReturnStatement(location(), functionCall(
                                        "CONCAT",
                                        literal("Hello, "),
                                        identifier("s"),
                                        literal("!")))),
                        false));
    }

    @Test
    void testEmptyFunction()
    {
        assertThat(statement("""
                CREATE OR REPLACE FUNCTION answer()
                RETURNS bigint
                RETURN 42
                """))
                .ignoringLocation()
                .isEqualTo(new CreateFunction(
                        location(),
                        new FunctionSpecification(
                                location(),
                                QualifiedName.of("answer"),
                                ImmutableList.of(),
                                returns(type("bigint")),
                                ImmutableList.of(),
                                new ReturnStatement(location(), literal(42))),
                        true));
    }

    @Test
    void testFibFunction()
    {
        assertThat(statement("""
                CREATE FUNCTION fib(n bigint)
                RETURNS bigint
                BEGIN
                  DECLARE a bigint DEFAULT 1;
                  DECLARE b bigint DEFAULT 1;
                  DECLARE c bigint;
                  IF n <= 2 THEN
                    RETURN 1;
                  END IF;
                  WHILE n > 2 DO
                    SET n = n - 1;
                    SET c = a + b;
                    SET a = b;
                    SET b = c;
                  END WHILE;
                  RETURN c;
                END
                """))
                .ignoringLocation()
                .isEqualTo(new CreateFunction(
                        location(),
                        new FunctionSpecification(
                                location(),
                                QualifiedName.of("fib"),
                                ImmutableList.of(parameter("n", type("bigint"))),
                                returns(type("bigint")),
                                ImmutableList.of(),
                                beginEnd(
                                        ImmutableList.of(
                                                declare("a", type("bigint"), literal(1)),
                                                declare("b", type("bigint"), literal(1)),
                                                declare("c", type("bigint"))),
                                        new IfStatement(
                                                location(),
                                                lte("n", literal(2)),
                                                ImmutableList.of(new ReturnStatement(location(), literal(1))),
                                                ImmutableList.of(),
                                                Optional.empty()),
                                        new WhileStatement(
                                                location(),
                                                Optional.empty(),
                                                gt("n", literal(2)),
                                                ImmutableList.of(
                                                        assign("n", minus(identifier("n"), literal(1))),
                                                        assign("c", plus(identifier("a"), identifier("b"))),
                                                        assign("a", identifier("b")),
                                                        assign("b", identifier("c")))),
                                        new ReturnStatement(location(), identifier("c")))),
                        false));
    }

    @Test
    void testFunctionWithIfElseIf()
    {
        assertThat(statement("""
                CREATE FUNCTION CustomerLevel(p_creditLimit DOUBLE)
                RETURNS varchar
                RETURNS NULL ON NULL INPUT
                SECURITY DEFINER
                BEGIN
                  DECLARE lvl VarChar;
                  IF p_creditLimit > 50000 THEN
                    SET lvl = 'PLATINUM';
                  ELSEIF (p_creditLimit <= 50000 AND p_creditLimit >= 10000) THEN
                    SET lvl = 'GOLD';
                  ELSEIF p_creditLimit < 10000 THEN
                    SET lvl = 'SILVER';
                  END IF;
                  RETURN (lvl);
                END
                """))
                .ignoringLocation()
                .isEqualTo(new CreateFunction(
                        location(),
                        new FunctionSpecification(
                                location(),
                                QualifiedName.of("CustomerLevel"),
                                ImmutableList.of(parameter("p_creditLimit", type("DOUBLE"))),
                                returns(type("varchar")),
                                ImmutableList.of(
                                        returnsNullOnNullInput(),
                                        new SecurityCharacteristic(location(), DEFINER)),
                                beginEnd(
                                        ImmutableList.of(declare("lvl", type("VarChar"))),
                                        new IfStatement(
                                                location(),
                                                gt("p_creditLimit", literal(50000)),
                                                ImmutableList.of(assign("lvl", literal("PLATINUM"))),
                                                ImmutableList.of(
                                                        elseIf(LogicalExpression.and(
                                                                        lte("p_creditLimit", literal(50000)),
                                                                        gte("p_creditLimit", literal(10000))),
                                                                assign("lvl", literal("GOLD"))),
                                                        elseIf(lt("p_creditLimit", literal(10000)),
                                                                assign("lvl", literal("SILVER")))),
                                                Optional.empty()),
                                        new ReturnStatement(location(), identifier("lvl")))),
                        false));
    }

    private static DataType type(String identifier)
    {
        return new GenericDataType(Optional.empty(), new Identifier(identifier, false), ImmutableList.of());
    }

    private static ReturnsClause returns(DataType type)
    {
        return new ReturnsClause(location(), type);
    }

    private static VariableDeclaration declare(String name, DataType type)
    {
        return new VariableDeclaration(location(), ImmutableList.of(new Identifier(name)), type, Optional.empty());
    }

    private static VariableDeclaration declare(String name, DataType type, Expression defaultValue)
    {
        return new VariableDeclaration(location(), ImmutableList.of(new Identifier(name)), type, Optional.of(defaultValue));
    }

    private static ParameterDeclaration parameter(String name, DataType type)
    {
        return new ParameterDeclaration(Optional.of(new Identifier(name)), type);
    }

    private static AssignmentStatement assign(String name, Expression value)
    {
        return new AssignmentStatement(location(), new Identifier(name), value);
    }

    private static ArithmeticBinaryExpression plus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, left, right);
    }

    private static ArithmeticBinaryExpression minus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, left, right);
    }

    private static ComparisonExpression lt(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, identifier(name), expression);
    }

    private static ComparisonExpression lte(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, identifier(name), expression);
    }

    private static ComparisonExpression gt(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, identifier(name), expression);
    }

    private static ComparisonExpression gte(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, identifier(name), expression);
    }

    private static StringLiteral literal(String literal)
    {
        return new StringLiteral(literal);
    }

    private static LongLiteral literal(long literal)
    {
        return new LongLiteral(String.valueOf(literal));
    }

    private static CompoundStatement beginEnd(List<VariableDeclaration> variableDeclarations, ControlStatement... statements)
    {
        return new CompoundStatement(location(), variableDeclarations, ImmutableList.copyOf(statements));
    }

    private static ElseIfClause elseIf(Expression expression, ControlStatement... statements)
    {
        return new ElseIfClause(expression, ImmutableList.copyOf(statements));
    }

    private static Query query(FunctionSpecification function, Select select)
    {
        return new Query(
                ImmutableList.of(function),
                Optional.empty(),
                new QuerySpecification(
                        select,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static NodeLocation location()
    {
        return new NodeLocation(1, 1);
    }
}
