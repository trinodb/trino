package com.aliyun.odps.cupid.trino;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.INTERNAL_ERROR;

public enum OdpsErrorCode implements ErrorCodeSupplier {
    ODPS_INTERNAL_ERROR(0, INTERNAL_ERROR),
    /* Shared error code with HiveErrorCode */;

    public static final int ERROR_CODE_MASK = 0x0100_0000;

    private final ErrorCode errorCode;

    OdpsErrorCode(int code, ErrorType type) {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
