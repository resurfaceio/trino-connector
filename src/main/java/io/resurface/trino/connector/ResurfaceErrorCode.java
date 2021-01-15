// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum ResurfaceErrorCode implements ErrorCodeSupplier {

    RESURFACE_NO_FILES(0, EXTERNAL),
    RESURFACE_FILESYSTEM_ERROR(1, EXTERNAL),
    RESURFACE_READ_ERROR(2, EXTERNAL);

    ResurfaceErrorCode(int code, ErrorType type) {
        errorCode = new ErrorCode(code + 0x0501_0000, name(), type);
    }

    private final ErrorCode errorCode;

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }

}
