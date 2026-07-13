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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonSimplifiedAccessor
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testMemberAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j.foo
                FROM (VALUES (JSON '{"foo":"bar"}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"bar\"]'");
    }

    @Test
    public void testMemberAccessorOnJsonStringYieldsNull()
    {
        // CAST(varchar AS JSON) produces a JSON string, not a parsed object. Member access on a JSON
        // scalar matches nothing, so the accessor navigates the value itself and yields NULL.
        assertThat(assertions.query(
                """
                SELECT j.foo
                FROM (VALUES (CAST('{"foo":"bar"}' AS JSON))) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testNestedMemberAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j.a.b
                FROM (VALUES (JSON '{"a":{"b":42}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[42]'");
    }

    @Test
    public void testObjectMemberNotWrapped()
    {
        assertThat(assertions.query(
                """
                SELECT j.a
                FROM (VALUES (JSON '{"a":{"b":42}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '{\"b\":42}'");
    }

    @Test
    public void testMissingMember()
    {
        assertThat(assertions.query(
                """
                SELECT j.absent
                FROM (VALUES (JSON '{"foo":"bar"}')) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testDelimitedMemberPreservesCase()
    {
        assertThat(assertions.query(
                """
                SELECT j."FooBar"
                FROM (VALUES (JSON '{"FooBar":"matched","foobar":"not matched"}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"matched\"]'");
    }

    @Test
    public void testNestedDelimitedMembers()
    {
        assertThat(assertions.query(
                """
                SELECT j."Outer Key"."Inner Key"
                FROM (VALUES (JSON '{"Outer Key":{"Inner Key":"matched","inner key":"not matched"}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"matched\"]'");

        // delimited and regular members compose in one chain
        assertThat(assertions.query(
                """
                SELECT j.wrapper."Inner Key".leaf
                FROM (VALUES (JSON '{"wrapper":{"Inner Key":{"leaf":42}}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[42]'");
    }

    @Test
    public void testNonJsonColumnIsRowFieldAccess()
    {
        assertThat(assertions.query(
                """
                WITH t(r) AS (SELECT CAST(ROW(1, 'a') AS ROW(x BIGINT, y VARCHAR)))
                SELECT r.x FROM t
                """))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testArrayIndexAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j[1]
                FROM (VALUES (JSON '[10, 20, 30]')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[20]'");
    }

    @Test
    public void testMemberThenIndex()
    {
        assertThat(assertions.query(
                """
                SELECT j.items[0]
                FROM (VALUES (JSON '{"items":["a","b","c"]}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"a\"]'");
    }

    @Test
    public void testIndexThenMember()
    {
        assertThat(assertions.query(
                """
                SELECT j[0].label
                FROM (VALUES (JSON '[{"label":"hi"}]')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"hi\"]'");
    }

    @Test
    public void testMixedMemberAndIndex()
    {
        assertThat(assertions.query(
                """
                SELECT j.rows[1].cells[0]
                FROM (VALUES (JSON '{"rows":[{"cells":[1]},{"cells":[42,43]}]}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[42]'");
    }

    @Test
    public void testIndexOutOfRangeYieldsNull()
    {
        assertThat(assertions.query(
                """
                SELECT j[99]
                FROM (VALUES (JSON '[1, 2, 3]')) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testNegativeIndex()
    {
        // the parser folds the sign into the literal, so the chain emits the path $[-1];
        // SQL/JSON path subscripts are non-negative, and in lax mode the structural error
        // is suppressed — the result is NULL, exactly as for an out-of-range index
        assertThat(assertions.query(
                """
                SELECT j[-1]
                FROM (VALUES (JSON '[1, 2, 3]')) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testArraySubscriptOnNonJsonStillWorks()
    {
        assertThat(assertions.query(
                """
                SELECT a[2] FROM (VALUES (ARRAY['a','b','c'])) AS t(a)
                """))
                .matches("VALUES CAST('b' AS VARCHAR(1))");
    }

    @Test
    public void testWildcardMemberAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j.*
                FROM (VALUES (JSON '{"a":1,"b":2}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1,2]'");
    }

    @Test
    public void testWildcardAfterMemberChain()
    {
        assertThat(assertions.query(
                """
                SELECT j.payload.*
                FROM (VALUES (JSON '{"payload":{"x":"a","y":"b"}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"a\",\"b\"]'");

        // member values of mixed types are all collected; a JSON value has canonical (sorted) key
        // order, so the members surface as v, w, x, y, z
        assertThat(assertions.query(
                """
                SELECT j.payload.*
                FROM (VALUES (JSON '{"payload":{"x":"a","y":true,"z":1,"w":[2],"v":{"k":3}}}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[{\"k\":3},[2],\"a\",true,1]'");
    }

    @Test
    public void testRelationAliasTakesPrecedenceOverJsonWildcard()
    {
        // t.* where t is both a relation alias and a JSON column expands the relation,
        // preserving pre-existing query semantics over the JSON accessor reading
        assertThat(assertions.query(
                """
                SELECT t.*
                FROM (VALUES (JSON '{"a":1}')) AS t(t)
                """))
                .matches("VALUES JSON '{\"a\":1}'");

        // with no relation named j in scope, the JSON wildcard applies to the column
        assertThat(assertions.query(
                """
                SELECT j.*
                FROM (VALUES (JSON '{"a":1}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1]'");
    }

    @Test
    public void testWildcardAfterArrayIndex()
    {
        assertThat(assertions.query(
                """
                SELECT j[0].*
                FROM (VALUES (JSON '[{"x":1,"y":2}]')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1,2]'");
    }

    @Test
    public void testWildcardOnRowColumnStillExpandsFields()
    {
        assertThat(assertions.query(
                """
                SELECT t.r.* FROM (VALUES ROW(CAST(ROW(1, 'a') AS ROW(x BIGINT, y VARCHAR)))) AS t(r)
                """))
                .matches("VALUES (BIGINT '1', VARCHAR 'a')");
    }

    @Test
    public void testBigintItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.n.bigint()
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testItemMethodPrecedenceOverMethodInvocation()
    {
        // The method-invocation feature shares the MethodCall surface with item-method
        // accessors. An item-method name on a JSON-typed receiver resolves as the item
        // method (SQL:2023 §6.36 case 3.a), shadowing any real method of the same name —
        // pinned here so the try-ordering in visitMethodCall is a contract, not an accident.
        assertThat(assertions.query(
                """
                SELECT j.n.bigint()
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .matches("VALUES BIGINT '42'");

        // a name that is not an item method falls through to real method resolution
        assertThat(assertions.query(
                """
                SELECT j.something(1)
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .failure()
                .hasMessageContaining("something");

        // an item-method name with an argument shape the accessor does not define
        // (bigint takes no arguments) must not be swallowed by the accessor either
        assertThat(assertions.query(
                """
                SELECT j.n.bigint(1)
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .failure()
                .hasMessageContaining("bigint");

        // a precision literal outside the int range is not an item-method form — it must
        // fall through rather than silently wrap into a different in-range precision
        assertThat(assertions.query(
                """
                SELECT j.n.decimal(4294967297)
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .failure()
                .hasMessageContaining("decimal");

        // item-method names are regular identifiers; a delimited name is never an item method
        assertThat(assertions.query(
                """
                SELECT j.n."BIGINT"()
                FROM (VALUES (JSON '{"n":42}')) AS t(j)
                """))
                .failure()
                .hasMessageContaining("not registered");
    }

    @Test
    public void testItemMethodDirectlyOnColumn()
    {
        // the 2-part FunctionCall shape: the item method applies to the column itself
        // (path `lax $`), with no member steps in between
        assertThat(assertions.query(
                """
                SELECT j.bigint()
                FROM (VALUES (JSON '42')) AS t(j)
                """))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testStringItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.label.string()
                FROM (VALUES (JSON '{"label":"hi"}')) AS t(j)
                """))
                .matches("VALUES CAST('hi' AS VARCHAR)");
    }

    @Test
    public void testBooleanItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.ok.boolean()
                FROM (VALUES (JSON '{"ok":true}')) AS t(j)
                """))
                .matches("VALUES TRUE");
    }

    @Test
    public void testIntegerItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.x.integer()
                FROM (VALUES (JSON '{"x":17}')) AS t(j)
                """))
                .matches("VALUES INTEGER '17'");
    }

    @Test
    public void testNumberItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.d.number()
                FROM (VALUES (JSON '{"d":3.5}')) AS t(j)
                """))
                .matches("VALUES DOUBLE '3.5'");
    }

    @Test
    public void testDecimalItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.price.decimal(10, 2)
                FROM (VALUES (JSON '{"price":19.99}')) AS t(j)
                """))
                .matches("VALUES CAST(19.99 AS DECIMAL(10, 2))");
    }

    @Test
    public void testTimestampItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.ts.timestamp(3)
                FROM (VALUES (JSON '{"ts":"2024-01-02 03:04:05.678"}')) AS t(j)
                """))
                .matches("VALUES TIMESTAMP '2024-01-02 03:04:05.678'");
    }

    @Test
    public void testItemMethodMissingMember()
    {
        assertThat(assertions.query(
                """
                SELECT j.missing.bigint()
                FROM (VALUES (JSON '{"present":1}')) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS BIGINT)");
    }

    @Test
    public void testItemMethodAfterSubscript()
    {
        assertThat(assertions.query(
                """
                SELECT j.items[1].bigint()
                FROM (VALUES (JSON '{"items":[10, 20, 30]}')) AS t(j)
                """))
                .matches("VALUES BIGINT '20'");
    }

    @Test
    public void testArrayWildcardAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j[*]
                FROM (VALUES (JSON '[10, 20, 30]')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[10,20,30]'");
    }

    @Test
    public void testMemberThenArrayWildcard()
    {
        assertThat(assertions.query(
                """
                SELECT j.items[*]
                FROM (VALUES (JSON '{"items":["a","b"]}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"a\",\"b\"]'");
    }

    @Test
    public void testArrayWildcardThenMember()
    {
        assertThat(assertions.query(
                """
                SELECT j.rows[*].x
                FROM (VALUES (JSON '{"rows":[{"x":1},{"x":2}]}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1,2]'");
    }

    @Test
    public void testArrayWildcardRejectedOnNonJson()
    {
        assertThat(assertions.query(
                """
                SELECT a[*] FROM (VALUES (ARRAY['a','b'])) AS t(a)
                """))
                .failure().hasMessageContaining("[*] array wildcard accessor is only allowed over a JSON simplified accessor chain");
    }

    @Test
    public void testArrayWildcardSurfacesColumnNotFound()
    {
        // When the base of an [*] reference doesn't resolve, the
        // column-not-found error is more useful than the catch-all
        // "[*] not allowed" message.
        assertThat(assertions.query(
                """
                SELECT badcol[*] FROM (VALUES (JSON '[1]')) AS t(j)
                """))
                .failure().hasMessageContaining("Column 'badcol' cannot be resolved");
    }

    @Test
    public void testStringLiteralMemberAccessor()
    {
        assertThat(assertions.query(
                """
                SELECT j.'foo'
                FROM (VALUES (JSON '{"foo":1}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1]'");
    }

    @Test
    public void testStringLiteralMemberWithSpecialChars()
    {
        assertThat(assertions.query(
                """
                SELECT j.'foo bar'
                FROM (VALUES (JSON '{"foo bar":"yes"}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[\"yes\"]'");
    }

    @Test
    public void testStringLiteralMemberWithItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.'count'.bigint()
                FROM (VALUES (JSON '{"count":42}')) AS t(j)
                """))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testUnquotedIdentifierIsCaseSensitiveInPath()
    {
        // SQL:2023 §6.36 NOTE 199: no implicit case folding. Unquoted `j.Foo`
        // emits `lax $.Foo` and matches only the literal `Foo` member.
        assertThat(assertions.query(
                """
                SELECT j.Foo
                FROM (VALUES (JSON '{"foo":1,"Foo":2}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[2]'");
    }

    @Test
    public void testDelimitedIdentifierIsCaseSensitiveInPath()
    {
        assertThat(assertions.query(
                """
                SELECT j."Foo"
                FROM (VALUES (JSON '{"foo":1,"Foo":2}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[2]'");
    }

    @Test
    public void testArrayWildcardThenMemberWildcardInSelectAll()
    {
        assertThat(assertions.query(
                """
                SELECT j[*].*
                FROM (VALUES (JSON '[{"a":1,"b":2}]')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1,2]'");
    }

    @Test
    public void testItemsArrayWildcardInSelectAll()
    {
        assertThat(assertions.query(
                """
                SELECT j.items[*].*
                FROM (VALUES (JSON '{"items":[{"a":1,"b":2}]}')) AS t(j)
                """))
                .matches("VALUES VARCHAR '[1,2]'");
    }

    @Test
    public void testDateItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.d.date()
                FROM (VALUES (JSON '{"d":"2024-01-02"}')) AS t(j)
                """))
                .matches("VALUES DATE '2024-01-02'");
    }

    @Test
    public void testTimeItemMethodDefaultPrecision()
    {
        assertThat(assertions.query(
                """
                SELECT j.t.time()
                FROM (VALUES (JSON '{"t":"03:04:05.678"}')) AS t(j)
                """))
                .matches("VALUES TIME '03:04:05.678'");
    }

    @Test
    public void testTimeItemMethodExplicitPrecision()
    {
        assertThat(assertions.query(
                """
                SELECT j.t.time(0)
                FROM (VALUES (JSON '{"t":"03:04:05"}')) AS t(j)
                """))
                .matches("VALUES TIME '03:04:05'");
    }

    @Test
    public void testTimeTzItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.t.time_tz(3)
                FROM (VALUES (JSON '{"t":"03:04:05.678+01:00"}')) AS t(j)
                """))
                .matches("VALUES TIME '03:04:05.678+01:00'");
    }

    @Test
    public void testTimestampTzItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT j.ts.timestamp_tz(3)
                FROM (VALUES (JSON '{"ts":"2024-01-02 03:04:05.678 UTC"}')) AS t(j)
                """))
                .matches("VALUES TIMESTAMP '2024-01-02 03:04:05.678 UTC'");
    }

    @Test
    public void testNegativeIndexYieldsNullInLaxMode()
    {
        assertThat(assertions.query(
                """
                SELECT j[-1]
                FROM (VALUES (JSON '[10, 20, 30]')) AS t(j)
                """))
                .matches("VALUES CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testOuterScopeJsonColumn()
    {
        // SQL:2023 §6.36 syntax rule 2 doesn't restrict VEP's scope —
        // any JSON-typed value expression is allowed, including a
        // correlated outer-scope column reference.
        assertThat(assertions.query(
                """
                SELECT (SELECT o.j.foo)
                FROM (VALUES (JSON '{"foo":1}')) AS o(j)
                """))
                .matches("VALUES VARCHAR '[1]'");
    }

    @Test
    public void testOuterScopeJsonColumnWithIndexedItemMethod()
    {
        assertThat(assertions.query(
                """
                SELECT (SELECT o.j.items[0].bigint())
                FROM (VALUES (JSON '{"items":[42]}')) AS o(j)
                """))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testOuterScopeJsonColumnWildcard()
    {
        assertThat(assertions.query(
                """
                SELECT (SELECT o.j.*)
                FROM (VALUES (JSON '{"foo":1}')) AS o(j)
                """))
                .matches("VALUES VARCHAR '[1]'");
    }

    @Test
    public void testOuterScopeJsonColumnFunctionItemMethodRejected()
    {
        // The FunctionCall item-method shape (no subscripts in the chain)
        // has no Expression sub-node distinct from the user's surface,
        // so the recipe can't hand a sub-tree to `outerContext.rewrite`.
        // Local-scope works; outer-scope rejects with a clean semantic
        // error rather than crashing in IR construction.
        assertThat(assertions.query(
                """
                SELECT (SELECT o.j.foo.bigint())
                FROM (VALUES (JSON '{"foo":42}')) AS o(j)
                """))
                .failure().hasMessageContaining("JSON simplified accessor over a column from an outer scope is not supported");
    }
}
