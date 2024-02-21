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

import io.starburst.schema.discovery.options.GeneralOptions;

import java.util.Map;

public interface FormatGuess
{
    enum Confidence
    {
        LOW,
        HIGH
    }

    Confidence confidence();

    default Map<String, String> options()
    {
        return GeneralOptions.DEFAULT_OPTIONS;
    }
}
