/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.starburst.schema.discovery.models.SlashEndedPath;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;

public class Errors
{
    private final Set<String> errors = Sets.newConcurrentHashSet();
    private final Map<String, Set<String>> tablePathToTableErrors = new ConcurrentHashMap<>();
    private static final Comparator<Map.Entry<String, Set<String>>> FILES_BEFORE_PARENTS_COMPARATOR = Comparator.<Map.Entry<String, Set<String>>>comparingInt(o -> o.getKey().length()).reversed();

    @FormatMethod
    public void addTableError(String withinTablePath, @FormatString String error, Object... args)
    {
        tablePathToTableErrors.computeIfAbsent(ensureEndsWithSlash(withinTablePath).toString(), __ -> Sets.newConcurrentHashSet())
                .add(String.format(error, args));
    }

    @FormatMethod
    public void addTableError(SlashEndedPath withinTablePath, @FormatString String error, Object... args)
    {
        tablePathToTableErrors.computeIfAbsent(withinTablePath.toString(), __ -> Sets.newConcurrentHashSet())
                .add(String.format(error, args));
    }

    public List<String> build()
    {
        return ImmutableList.copyOf(errors);
    }

    public List<String> buildForPathAndChildren(String path)
    {
        String slashEndedBasePath = ensureEndsWithSlash(path).toString();
        return tablePathToTableErrors.entrySet().stream()
                .filter(e -> e.getKey().startsWith(slashEndedBasePath))
                .sorted(FILES_BEFORE_PARENTS_COMPARATOR)
                .flatMap(e -> e.getValue().stream())
                .distinct()
                .collect(toImmutableList());
    }

    public Map<String, List<String>> buildPathErrors()
    {
        return tablePathToTableErrors.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
    }
}
