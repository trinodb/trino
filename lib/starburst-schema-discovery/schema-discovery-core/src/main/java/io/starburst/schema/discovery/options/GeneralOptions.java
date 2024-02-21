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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.infer.OptionDescription;
import io.starburst.schema.discovery.models.TableFormat;

import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class GeneralOptions
{
    public static final String PATTERN_SPLITTER = "|";
    @OptionDescription("Charset to use when reading files")
    public static final String CHARSET = "encoding";

    @OptionDescription("Locale for date parsing")
    public static final String LOCALE = "locale";

    @OptionDescription("Date format pattern")
    public static final String DATE_FORMAT = "dateFormat";

    @OptionDescription("Time format pattern")
    public static final String TIMESTAMP_FORMAT = "timestampFormat";

    @OptionDescription("Value to use for not-a-number")
    public static final String NAN_VALUE = "nanValue";

    @OptionDescription("Value to use for positive infinity")
    public static final String POSITIVE_INF = "positiveInf";

    @OptionDescription("Value to use for negative infinity")
    public static final String NEGATIVE_INF = "negativeInf";

    @OptionDescription("HDFS include GLOB, split by |")
    public static final String INCLUDE_PATTERNS = "includePatterns";

    @OptionDescription("HDFS exclude GLOB, split by |")
    public static final String EXCLUDE_PATTERNS = "excludePatterns";

    @OptionDescription("Max lines to sample from each file")
    public static final String MAX_SAMPLE_LINES = "maxSampleLines";

    @OptionDescription("Each SAMPLE_LINES_MODULO line is sampled. i.e. if 3, every 3rd line is sampled")
    public static final String SAMPLE_LINES_MODULO = "sampleLinesModulo";

    @OptionDescription("Each SAMPLE_FILES_PER_TABLE_MODULO file is sampled. i.e. if 3, every 3rd file is sampled")
    public static final String SAMPLE_FILES_PER_TABLE_MODULO = "sampleFilesPerTableModulo";

    @OptionDescription("Max Tables to discovery")
    public static final String MAX_SAMPLE_TABLES = "maxSampleTables";

    @OptionDescription("Max files per table to sample")
    public static final String MAX_SAMPLE_FILES_PER_TABLE = "maxSampleFilesPerTable";

    @OptionDescription("If \"true\" attempt to infer buckets")
    public static final String LOOK_FOR_BUCKETS = "supportBuckets";

    @OptionDescription(value = "Optional = force the table format", enums = TableFormat.class)
    public static final String FORCED_FORMAT = "forceTableFormat";

    @OptionDescription(value = "Discovery mode, NORMAL - default discovery mode, RECURSIVE_DIRECTORIES - ignores directories in top level tables", enums = DiscoveryMode.class)
    public static final String DISCOVERY_MODE = "discoveryMode";

    @OptionDescription("If \"true\" attempt to infer partition projection")
    public static final String DISCOVER_PARTITION_PROJECTION = "partitionProjection";

    @OptionDescription(value = "Whether to skip looking at file extension when discovering file format. Use in case of mismatched file format / extension")
    public static final String SKIP_FILE_EXTENSIONS_CHECK = "skipFileExtensionsCheck";

    @OptionDescription(value = "Try to parse string json values into Trino's DECIMAL type")
    public static final String INFER_JSON_STRING_TO_DECIMAL = "inferJsonStringDecimal";

    @OptionDescription(value = "Whether to skip files/directories starting with _ or .")
    public static final String HIVE_SKIP_HIDDEN_FILES = "hiveSkipHiddenFiles";

    protected final OptionsMap options;

    public static final Map<String, String> DEFAULT_OPTIONS = ImmutableMap.<String, String>builder()
            .put(CHARSET, Charset.defaultCharset().name())
            .put(DATE_FORMAT, "yyyy-MM-dd")
            .put(TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss[.SSSSSSS]")
            .put(NAN_VALUE, "NaN")
            .put(POSITIVE_INF, "Inf")
            .put(NEGATIVE_INF, "-Inf")
            .put(MAX_SAMPLE_LINES, "10")
            .put(SAMPLE_LINES_MODULO, "3")
            .put(LOCALE, "US")
            .put(INCLUDE_PATTERNS, "**")
            .put(EXCLUDE_PATTERNS, ".*")
            // make the default irrelevant, so by default we will rely on client's timeout, as general desire would be to discover all tables or failed,
            // rather than silently ignore some
            .put(MAX_SAMPLE_TABLES, String.valueOf(Integer.MAX_VALUE))
            .put(MAX_SAMPLE_FILES_PER_TABLE, "5")
            .put(SAMPLE_FILES_PER_TABLE_MODULO, "3")
            .put(LOOK_FOR_BUCKETS, "false")
            .put(DISCOVERY_MODE, DiscoveryMode.NORMAL.name())
            .put(DISCOVER_PARTITION_PROJECTION, "false")
            .put(SKIP_FILE_EXTENSIONS_CHECK, "false")
            .put(INFER_JSON_STRING_TO_DECIMAL, "false")
            .put(HIVE_SKIP_HIDDEN_FILES, "false")
            .buildOrThrow();

    protected static Map<String, String> combinedDefaults(Map<String, String> defaults)
    {
        Map<String, String> combined = new HashMap<>(DEFAULT_OPTIONS);
        combined.putAll(defaults);
        return combined;
    }

    public GeneralOptions(OptionsMap options)
    {
        this(options, DEFAULT_OPTIONS);
    }

    protected GeneralOptions(OptionsMap options, Map<String, String> defaultOptions)
    {
        this.options = options.withDefaults(defaultOptions);
    }

    public Charset charset()
    {
        return Charset.forName(options.get(CHARSET));
    }

    public Locale locale()
    {
        return options.locale(LOCALE);
    }

    public String dateFormat()
    {
        return options.get(DATE_FORMAT);
    }

    public DateTimeFormatter dateFormatter()
    {
        return options.dateFormatter(DATE_FORMAT, locale());
    }

    public DateTimeFormatter dateTimeFormatter()
    {
        return options.dateTimeFormatter(TIMESTAMP_FORMAT, locale());
    }

    public DecimalFormat decimalFormat()
    {
        return options.decimalFormat(locale());
    }

    public String nanValue()
    {
        return options.get(NAN_VALUE);
    }

    public String positiveInf()
    {
        return options.get(POSITIVE_INF);
    }

    public String negativeInf()
    {
        return options.get(NEGATIVE_INF);
    }

    public int maxSampleLines()
    {
        return options.integer(MAX_SAMPLE_LINES);
    }

    public int sampleLinesModulo()
    {
        return options.integer(SAMPLE_LINES_MODULO);
    }

    public int maxSampleFilesPerTable()
    {
        return options.integer(MAX_SAMPLE_FILES_PER_TABLE);
    }

    public int sampleFilesPerTableModulo()
    {
        return options.integer(SAMPLE_FILES_PER_TABLE_MODULO);
    }

    public int maxSampleTables()
    {
        return options.integer(MAX_SAMPLE_TABLES);
    }

    public List<String> includePatterns()
    {
        return Splitter.on(PATTERN_SPLITTER).omitEmptyStrings().trimResults().splitToList(options.get(INCLUDE_PATTERNS));
    }

    public List<String> excludePatterns()
    {
        return Splitter.on(PATTERN_SPLITTER).omitEmptyStrings().trimResults().splitToList(options.get(EXCLUDE_PATTERNS));
    }

    public boolean lookForBuckets()
    {
        return options.bool(LOOK_FOR_BUCKETS);
    }

    public Optional<TableFormat> forcedFormat()
    {
        String forcedFormat = options.get(FORCED_FORMAT);
        if (forcedFormat != null) {
            return Optional.of(TableFormat.valueOf(forcedFormat.toUpperCase(Locale.ENGLISH)));
        }
        return Optional.empty();
    }

    public DiscoveryMode discoveryMode()
    {
        return Optional.ofNullable(options.get(DISCOVERY_MODE))
                .map(String::toUpperCase)
                .map(DiscoveryMode::valueOf)
                .orElse(DiscoveryMode.NORMAL);
    }

    public boolean discoverPartitionProjection()
    {
        return options.bool(DISCOVER_PARTITION_PROJECTION);
    }

    public boolean skipFileExtensionCheck()
    {
        return options.bool(SKIP_FILE_EXTENSIONS_CHECK);
    }

    public boolean inferJsonStringToDecimal()
    {
        return options.bool(INFER_JSON_STRING_TO_DECIMAL);
    }

    public boolean skipHiddenFiles()
    {
        return options.bool(HIVE_SKIP_HIDDEN_FILES);
    }
}
