/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.infer;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.util.Optional;

class DecimalParser
{
    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ExprUtils.scala#L85
    static Optional<BigDecimal> parse(DecimalFormat decimalFormat, String s)
    {
        ParsePosition position = new ParsePosition(0);
        Number parsed = decimalFormat.parse(s, position);
        if ((position.getIndex() != s.length()) || (position.getErrorIndex() != -1)) {
            return Optional.empty();
        }
        if (parsed instanceof BigDecimal bigDecimal) {
            return Optional.of(bigDecimal);
        }
        if (Double.isNaN(parsed.doubleValue())) {
            return Optional.empty();
        }
        if (Double.isInfinite(parsed.doubleValue())) {
            return Optional.empty();
        }
        return Optional.of(BigDecimal.valueOf(parsed.doubleValue()));
    }

    private DecimalParser()
    {
    }
}
