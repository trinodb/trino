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
import io.starburst.schema.discovery.TableChanges.TableName;
import io.trino.spi.TrinoException;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.UNEXPECTED_DATA_TYPE;

public class OptionsMap
{
    private final Map<String, String> overrides;
    private final Map<String, String> defaults;

    public OptionsMap()
    {
        this(ImmutableMap.of(), ImmutableMap.of());
    }

    public OptionsMap(Map<String, String> overrides)
    {
        this(overrides, ImmutableMap.of());
    }

    public OptionsMap withDefaults(Map<String, String> defaults)
    {
        return new OptionsMap(overrides, defaults);
    }

    public OptionsMap withTableName(TableName tableName)
    {
        String prefix = tableName + ".";
        Map<String, String> standardOverrides = new HashMap<>();
        Map<String, String> tableOverrides = new HashMap<>();
        overrides.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                tableOverrides.put(key.substring(prefix.length()), value);
            }
            else {
                standardOverrides.put(key, value);
            }
        });
        Map<String, String> applied = new HashMap<>(defaults);
        applied.putAll(standardOverrides);
        applied.putAll(tableOverrides);
        return new OptionsMap(applied, defaults);
    }

    public Map<String, String> unwrap()
    {
        Map<String, String> unwrapped = new HashMap<>(defaults);
        unwrapped.putAll(overrides);
        return ImmutableMap.copyOf(unwrapped);
    }

    public Locale locale(String key)
    {
        String localeName = get(key);
        return Stream.of(Locale.getAvailableLocales())
                .filter(locale -> locale.getCountry().equals(localeName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Locale not found: " + localeName));
    }

    public DateTimeFormatter dateTimeFormatter(String key, Locale locale)
    {
        return DateTimeFormatter.ofPattern(get(key), locale);
    }

    public DateTimeFormatter dateFormatter(String key, Locale locale)
    {
        return DateTimeFormatter.ofPattern(get(key), locale);
    }

    public DecimalFormat decimalFormat(Locale locale)
    {
        DecimalFormat decimalFormat = new DecimalFormat("", new DecimalFormatSymbols(locale));
        decimalFormat.setParseBigDecimal(true);
        return decimalFormat;
    }

    public boolean bool(String key)
    {
        String value = get(key);
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new TrinoException(UNEXPECTED_DATA_TYPE, "Bad boolean value: " + value);
    }

    public int integer(String key)
    {
        String value = get(key);
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(UNEXPECTED_DATA_TYPE, "Bad integer value: " + value);
        }
    }

    public char firstChar(String key)
    {
        String value = get(key);
        checkArgument(value.length() == 1, "Value must be a single character: " + value);
        return value.charAt(0);
    }

    public String get(String key)
    {
        key = key.toLowerCase(Locale.getDefault());
        String value = overrides.get(key);
        return (value != null) ? value : defaults.get(key);
    }

    private static Map<String, String> lcase(Map<String, String> map)
    {
        return map.entrySet().stream().collect(toImmutableMap(entry -> entry.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
    }

    private OptionsMap(Map<String, String> overrides, Map<String, String> defaults)
    {
        this.overrides = lcase(overrides);
        this.defaults = lcase(defaults);
    }
}
