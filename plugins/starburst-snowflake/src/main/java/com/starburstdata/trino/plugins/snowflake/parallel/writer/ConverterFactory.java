/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel.writer;

import com.starburstdata.trino.plugins.snowflake.parallel.StarburstDataConversionContext;
import io.trino.spi.TrinoException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.core.arrow.BigIntToFixedConverter;
import net.snowflake.client.core.arrow.BigIntToScaledFixedConverter;
import net.snowflake.client.core.arrow.BigIntToTimeConverter;
import net.snowflake.client.core.arrow.BigIntToTimestampLTZConverter;
import net.snowflake.client.core.arrow.BigIntToTimestampNTZConverter;
import net.snowflake.client.core.arrow.BitToBooleanConverter;
import net.snowflake.client.core.arrow.DateConverter;
import net.snowflake.client.core.arrow.DecimalToScaledFixedConverter;
import net.snowflake.client.core.arrow.DoubleToRealConverter;
import net.snowflake.client.core.arrow.IntToFixedConverter;
import net.snowflake.client.core.arrow.IntToScaledFixedConverter;
import net.snowflake.client.core.arrow.IntToTimeConverter;
import net.snowflake.client.core.arrow.SmallIntToFixedConverter;
import net.snowflake.client.core.arrow.SmallIntToScaledFixedConverter;
import net.snowflake.client.core.arrow.ThreeFieldStructToTimestampTZConverter;
import net.snowflake.client.core.arrow.TinyIntToFixedConverter;
import net.snowflake.client.core.arrow.TinyIntToScaledFixedConverter;
import net.snowflake.client.core.arrow.TwoFieldStructToTimestampLTZConverter;
import net.snowflake.client.core.arrow.TwoFieldStructToTimestampNTZConverter;
import net.snowflake.client.core.arrow.TwoFieldStructToTimestampTZConverter;
import net.snowflake.client.core.arrow.VarBinaryToBinaryConverter;
import net.snowflake.client.core.arrow.VarCharConverter;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.Types;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.Field;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static net.snowflake.client.jdbc.internal.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

public final class ConverterFactory
{
    private ConverterFactory()
    {
        // static utility class
    }

    /**
     * Copied from {@link net.snowflake.client.jdbc.ArrowResultChunk#initConverters}
     *
     * Given an arrow vector (column in a single record batch), return arrow
     * vector converter. Note, converter is built on top of arrow vector, so that arrow data can be
     * converted back to java
     * <p>
     * Arrow converter mappings for Snowflake fixed-point numbers
     * -----------------------------------------------------------------------------------------<br>
     * Max position and scale Converter
     * -----------------------------------------------------------------------------------------<br>
     * number(3,0) {@link TinyIntToFixedConverter} number(3,2) {@link TinyIntToScaledFixedConverter}
     * number(5,0) {@link SmallIntToFixedConverter} number(5,4) {@link SmallIntToScaledFixedConverter}
     * number(10,0) {@link IntToFixedConverter} number(10,9) {@link IntToScaledFixedConverter}
     * number(19,0) {@link BigIntToFixedConverter} number(19,18) {@link BigIntToFixedConverter}
     * number(38,37) {@link DecimalToScaledFixedConverter}
     */
    public static ArrowVectorConverter createSnowflakeConverter(ValueVector vector, int index, StarburstDataConversionContext conversionContext)
    {
        Field field = vector.getField();
        Types.MinorType arrowMinorType = getMinorTypeForArrowType(field.getType());
        Map<String, String> columnMetadata = field.getMetadata();
        if (arrowMinorType == Types.MinorType.DECIMAL) {
            // Note: Decimal vector is different from others
            return new DecimalToScaledFixedConverter(vector, index, conversionContext);
        }
        else if (!columnMetadata.isEmpty()) {
            SnowflakeType snowflakeType = SnowflakeType.valueOf(columnMetadata.get("logicalType"));
            switch (snowflakeType) {
                case ANY, ARRAY, CHAR, TEXT, OBJECT, VARIANT -> {
                    return new VarCharConverter(vector, index, conversionContext);
                }
                case BINARY -> {
                    return new VarBinaryToBinaryConverter(vector, index, conversionContext);
                }
                case BOOLEAN -> {
                    return new BitToBooleanConverter(vector, index, conversionContext);
                }
                case DATE -> {
                    return new DateConverter(vector, index, conversionContext);
                }
                case FIXED -> {
                    int scale = Integer.parseInt(field.getMetadata().get("scale"));
                    switch (arrowMinorType) {
                        case TINYINT -> {
                            if (scale == 0) {
                                return new TinyIntToFixedConverter(vector, index, conversionContext);
                            }
                            else {
                                return new TinyIntToScaledFixedConverter(vector, index, conversionContext, scale);
                            }
                        }
                        case SMALLINT -> {
                            if (scale == 0) {
                                return new SmallIntToFixedConverter(vector, index, conversionContext);
                            }
                            else {
                                return new SmallIntToScaledFixedConverter(vector, index, conversionContext, scale);
                            }
                        }
                        case INT -> {
                            if (scale == 0) {
                                return new IntToFixedConverter(vector, index, conversionContext);
                            }
                            else {
                                return new IntToScaledFixedConverter(vector, index, conversionContext, scale);
                            }
                        }
                        case BIGINT -> {
                            if (scale == 0) {
                                return new BigIntToFixedConverter(vector, index, conversionContext);
                            }
                            else {
                                return new BigIntToScaledFixedConverter(vector, index, conversionContext, scale);
                            }
                        }
                    }
                }
                case REAL -> {
                    return new DoubleToRealConverter(vector, index, conversionContext);
                }
                case TIME -> {
                    switch (arrowMinorType) {
                        case INT -> {
                            return new IntToTimeConverter(vector, index, conversionContext);
                        }
                        case BIGINT -> {
                            return new BigIntToTimeConverter(vector, index, conversionContext);
                        }
                        default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected Arrow Field for %s".formatted(snowflakeType.name()));
                    }
                }
                case TIMESTAMP_LTZ -> {
                    if (field.getChildren().isEmpty()) {
                        // case when the scale of the timestamp is equal or smaller than millisecs since epoch
                        return new BigIntToTimestampLTZConverter(vector, index, conversionContext);
                    }
                    else if (field.getChildren().size() == 2) {
                        // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
                        // nanosecs
                        return new TwoFieldStructToTimestampLTZConverter(vector, index, conversionContext);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected Arrow Field for %s".formatted(snowflakeType.name()));
                    }
                }
                case TIMESTAMP_NTZ -> {
                    if (field.getChildren().isEmpty()) {
                        // case when the scale of the timestamp is equal or smaller than 7
                        return new BigIntToTimestampNTZConverter(vector, index, conversionContext);
                    }
                    else if (field.getChildren().size() == 2) {
                        // when the timestamp is represented in two-field struct
                        return new TwoFieldStructToTimestampNTZConverter(vector, index, conversionContext);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected Arrow Field for %s".formatted(snowflakeType.name()));
                    }
                }
                case TIMESTAMP_TZ -> {
                    if (field.getChildren().size() == 2) {
                        // case when the scale of the timestamp is equal or smaller than millisecs since epoch
                        return new TwoFieldStructToTimestampTZConverter(vector, index, conversionContext);
                    }
                    else if (field.getChildren().size() == 3) {
                        // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
                        // nanosecs
                        return new ThreeFieldStructToTimestampTZConverter(vector, index, conversionContext);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected SnowflakeType %s".formatted(snowflakeType.name()));
                    }
                }
                default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected Arrow Field for %s".formatted(snowflakeType.name()));
            }
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected Arrow Field for %s".formatted(arrowMinorType.toString()));
    }
}
