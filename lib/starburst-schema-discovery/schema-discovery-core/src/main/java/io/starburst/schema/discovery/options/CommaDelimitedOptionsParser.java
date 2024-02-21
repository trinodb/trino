/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.options;

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.infer.OptionDescription;
import io.trino.spi.TrinoException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class CommaDelimitedOptionsParser
{
    private final Set<String> optionNames;

    record OptionNameIndex(String name, int index) {}

    public CommaDelimitedOptionsParser(List<Class<? extends GeneralOptions>> optionsClasses)
    {
        optionNames = optionsClasses.stream()
                .flatMap(optionsClass -> Arrays.stream(optionsClass.getDeclaredFields()))
                .filter(field -> field.isAnnotationPresent(OptionDescription.class) &&
                                 Modifier.isStatic(field.getModifiers()) &&
                                 Modifier.isPublic(field.getModifiers()) &&
                                 Modifier.isFinal(field.getModifiers()) &&
                                 field.getType().equals(String.class))
                .map(CommaDelimitedOptionsParser::extractStringValue)
                .filter(Predicate.not(String::isEmpty))
                .collect(toImmutableSet());

        if (optionNames.stream().anyMatch(n -> n.contains(",") || n.contains("="))) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Option names cannot have delimiting characters ,=");
        }
    }

    public Map<String, String> parse(String optionsString)
    {
        if (optionsString == null || optionsString.isBlank()) {
            return ImmutableMap.of();
        }

        List<OptionNameIndex> sortedMatchingOptions = optionNames.stream()
                .flatMap(name -> allIndexesOf(optionsString, name)
                        .map(index -> new OptionNameIndex(name, index)))
                .filter(nameIndex -> nameIndex.index() >= 0)
                .sorted(Comparator.comparingInt(OptionNameIndex::index))
                .collect(toImmutableList());

        return buildMatchingOptionsMap(optionsString, sortedMatchingOptions);
    }

    private Stream<Integer> allIndexesOf(String subject, String lookup)
    {
        // add key value separator to option name, as table name / schema name might match our option name,
        // and we want index of only latest bit before value itself
        String lookupWithSeparator = lookup + "=";
        return IntStream
                .iterate(subject.indexOf(lookupWithSeparator), index -> index >= 0, index -> subject.indexOf(lookupWithSeparator, index + 1))
                .boxed();
    }

    private static ImmutableMap<String, String> buildMatchingOptionsMap(String optionsString, List<OptionNameIndex> sortedMatchingOptions)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();
        int indexAfterLastParsedValue = 0;

        for (int i = 0; i < sortedMatchingOptions.size(); i++) {
            OptionNameIndex current = sortedMatchingOptions.get(i);
            Optional<OptionNameIndex> nextMaybe = (i + 1) == sortedMatchingOptions.size() ? Optional.empty() : Optional.of(sortedMatchingOptions.get(i + 1));

            int currentValueStartIndex = current.index() + current.name().length() + 1; // 1 for key-value separator (= by default)
            int currentValueEndOffset = nextMaybe.map(next -> optionsString.substring(currentValueStartIndex, next.index()).lastIndexOf(",")).orElse(optionsString.length() - currentValueStartIndex);

            int currentNameEndIndex = current.index() + current.name().length();

            optionsBuilder.put(
                    optionsString.substring(indexAfterLastParsedValue, currentNameEndIndex), // need to account for prefix in names, start from what previously was end
                    optionsString.substring(currentValueStartIndex, currentValueStartIndex + currentValueEndOffset));
            indexAfterLastParsedValue = currentValueStartIndex + currentValueEndOffset + 1; // 1 for options delimiter
        }
        return optionsBuilder.buildOrThrow();
    }

    private static String extractStringValue(Field field)
    {
        try {
            return (String) field.get(null);
        }
        catch (IllegalAccessException e) {
            return "";
        }
    }
}
