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
package io.trino.plugin.openpolicyagent;

import com.google.common.collect.Streams;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Collection;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.trino.plugin.openpolicyagent.TestHelpers.createIllegalResponseTestCases;

public class FilteringTestHelpers
{
    private FilteringTestHelpers() {}

    public static Stream<Arguments> emptyInputTestCases()
    {
        Stream<BiFunction<OpaAccessControl, SystemSecurityContext, Collection>> callables = Stream.of(
                (authorizer, context) -> authorizer.filterViewQueryOwnedBy(context, Set.<Identity>of()),
                (authorizer, context) -> authorizer.filterCatalogs(context, Set.of()),
                (authorizer, context) -> authorizer.filterSchemas(context, "some-catalog", Set.of()),
                (authorizer, context) -> authorizer.filterTables(context, "some-catalog", Set.of()),
                (authorizer, context) -> authorizer.filterColumns(
                        context,
                        new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"),
                        Set.of()));
        Stream<String> testNames = Stream.of("filterViewQueryOwnedBy", "filterCatalogs", "filterSchemas", "filterTables", "filterColumns");
        return Streams.zip(testNames, callables, (name, method) -> Arguments.of(Named.of(name, method)));
    }

    public static Stream<Arguments> prepopulatedErrorCases()
    {
        Stream<BiFunction<OpaAccessControl, SystemSecurityContext, Collection>> callables = Stream.of(
                (authorizer, context) -> authorizer.filterViewQueryOwnedBy(context, Set.of(Identity.ofUser("foo"))),
                (authorizer, context) -> authorizer.filterCatalogs(context, Set.of("foo")),
                (authorizer, context) -> authorizer.filterSchemas(context, "some-catalog", Set.of("foo")),
                (authorizer, context) -> authorizer.filterTables(context, "some-catalog", Set.of(new SchemaTableName("foo", "bar"))),
                (authorizer, context) -> authorizer.filterColumns(
                        context,
                        new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"),
                        Set.of("foo")));
        Stream<String> testNames = Stream.of("filterViewQueryOwnedBy", "filterCatalogs", "filterSchemas", "filterTables", "filterColumns");
        return createIllegalResponseTestCases(Streams.zip(testNames, callables, (name, method) -> Arguments.of(Named.of(name, method))));
    }
}
