package io.prestosql.plugin.influx;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.PrestoException;

public enum InfluxErrorCode implements ErrorCodeSupplier {

    GENERAL_ERROR(1, ErrorType.EXTERNAL),
    IDENTIFIER_CASE_SENSITIVITY(2, ErrorType.EXTERNAL);

    private static final int ERROR_BASE = 0;  // FIXME needs allocating
    private ErrorCode errorCode;

    InfluxErrorCode(int code, ErrorType type) {
        this.errorCode = new ErrorCode(code + ERROR_BASE, name(), ErrorType.EXTERNAL);
    }

    public void check(boolean condition, String message, String context) {
        if (!condition) {
            fail(message, context);
        }
    }

    public void check(boolean condition, String message) {
        check(condition, message, null);
    }

    public void fail(String message, String context) {
        throw new PrestoException(this, message + (context != null && !context.isEmpty()? " " + context: ""));
    }

    public void fail(String message) {
        fail(message, null);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
