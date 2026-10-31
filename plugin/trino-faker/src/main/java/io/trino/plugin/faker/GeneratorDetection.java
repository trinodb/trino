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

package io.trino.plugin.faker;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

final class GeneratorDetection
{
    private static final CharMatcher IGNORED_CHARACTERS = CharMatcher.anyOf("_- ");

    // Faker doesn't truncate generated values, so a bounded column must be at least minColumnLength long.
    // For values of variable length, the minimum leaves room above the longest values generated in any Datafaker locale.
    static final List<NamedGenerator> GENERATORS = ImmutableList.of(
            new NamedGenerator(List.of("email", "email_address"), "#{Internet.emailAddress}", 60),
            new NamedGenerator(List.of("first_name", "given_name"), "#{Name.first_name}", 30),
            new NamedGenerator(List.of("last_name", "family_name", "surname"), "#{Name.last_name}", 50),
            new NamedGenerator(List.of("full_name"), "#{Name.first_name} #{Name.last_name}", 60),
            new NamedGenerator(List.of("username"), "#{Internet.username}", 60),
            new NamedGenerator(List.of("street_address"), "#{Address.streetAddress}", 100),
            new NamedGenerator(List.of("city"), "#{Address.city}", 60),
            new NamedGenerator(List.of("country"), "#{Address.country}", 60),
            new NamedGenerator(List.of("country_code"), "#{Country.countryCode2}", 2),
            new NamedGenerator(List.of("postal_code", "postcode", "zip", "zip_code"), "#{Address.zipCode}", 10),
            new NamedGenerator(List.of("company", "company_name"), "#{Company.name}", 120),
            new NamedGenerator(List.of("job_title"), "#{Job.title}", 100),
            new NamedGenerator(List.of("website", "url"), "https://#{Internet.domainName}", 150),
            new NamedGenerator(List.of("currency", "currency_code"), "#{Currency.code}", 3),
            new NamedGenerator(List.of("iban"), "#{Finance.iban}", 34),
            new NamedGenerator(List.of("ip_address"), "#{Internet.ipV4Address}", 15));

    private static final Map<String, NamedGenerator> GENERATORS_BY_NAME = GENERATORS.stream()
            .flatMap(generator -> generator.columnNames().stream()
                    .map(columnName -> Map.entry(normalize(columnName), generator)))
            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    private GeneratorDetection() {}

    static Optional<String> detectGenerator(String columnName, Type type)
    {
        NamedGenerator generator = GENERATORS_BY_NAME.get(normalize(columnName));
        if (generator == null || !isLongEnough(type, generator.minColumnLength())) {
            return Optional.empty();
        }
        return Optional.of(generator.expression());
    }

    private static String normalize(String columnName)
    {
        return IGNORED_CHARACTERS.removeFrom(columnName.toLowerCase(ENGLISH));
    }

    private static boolean isLongEnough(Type type, int minColumnLength)
    {
        return switch (type) {
            case VarcharType varcharType -> varcharType.isUnbounded() || varcharType.getBoundedLength() >= minColumnLength;
            case CharType charType -> charType.getLength() >= minColumnLength;
            default -> false;
        };
    }

    record NamedGenerator(List<String> columnNames, String expression, int minColumnLength)
    {
        NamedGenerator
        {
            columnNames = ImmutableList.copyOf(columnNames);
            requireNonNull(expression, "expression is null");
        }
    }
}
