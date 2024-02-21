/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum SchemaDiscoveryErrorCode
        implements ErrorCodeSupplier
{
    UNEXPECTED_DATA_TYPE(0, EXTERNAL),
    UNION_NOT_SUPPORTED(1, EXTERNAL),
    UNEXPECTED_COLUMN_STATE(2, EXTERNAL),
    IO(3, EXTERNAL),
    NOT_A_DIRECTORY(4, EXTERNAL),
    BAD_FILTER(5, EXTERNAL),
    INTERNAL(6, INTERNAL_ERROR),
    INVALID_METADATA(7, USER_ERROR),
    LOCATION_DOES_NOT_EXISTS(8, EXTERNAL);

    private final ErrorCode errorCode;

    SchemaDiscoveryErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0101_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
