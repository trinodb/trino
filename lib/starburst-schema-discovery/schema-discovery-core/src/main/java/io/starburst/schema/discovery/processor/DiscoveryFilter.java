/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.trino.filesystem.Location;
import io.trino.spi.TrinoException;

import java.util.Set;
import java.util.function.Predicate;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.BAD_FILTER;
import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class DiscoveryFilter
        implements Predicate<Location>
{
    private static final Set<Character> HIDDEN_FILES_PREFIX = ImmutableSet.of('_', '.');
    private static final Predicate<Location> IGNORE_HIDDEN_FILES = not(location -> HIDDEN_FILES_PREFIX.contains(directoryOrFileName(location).charAt(0)));

    private final OptionsMap options;
    private final Predicate<Location> delegate;

    public DiscoveryFilter(OptionsMap options)
    {
        this.options = requireNonNull(options, "options is null");
        this.delegate = filter();
    }

    @Override
    public boolean test(Location location)
    {
        return delegate.test(location);
    }

    private Predicate<Location> filter()
    {
        ImmutableList.Builder<Predicate<Location>> builder = ImmutableList.builder();
        GeneralOptions generalOptions = new GeneralOptions(options);
        generalOptions.includePatterns().forEach(pattern -> builder.add(buildGlobFilter(pattern)));
        generalOptions.excludePatterns().forEach(pattern -> builder.add(buildGlobFilter(pattern).negate()));
        if (generalOptions.skipHiddenFiles()) {
            builder.add(IGNORE_HIDDEN_FILES);
        }

        ImmutableList<Predicate<Location>> filters = builder.build();
        return path -> filters.stream().allMatch(filter -> filter.test(path));
    }

    private Predicate<Location> buildGlobFilter(String pattern)
    {
        try {
            GlobPattern globPattern = new GlobPattern(pattern);

            return location -> globPattern.matches(location.path());
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new TrinoException(BAD_FILTER, "Bad GLOB pattern: " + pattern, e);
        }
    }
}
