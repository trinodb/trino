package io.trino.loki;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum LokiErrorCode
    implements ErrorCodeSupplier
{

    LOKI_UNKNOWN_ERROR(0, EXTERNAL);

    private final ErrorCode errorCode;

    LokiErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0509_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
