// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum LocalFileErrorCode
        implements ErrorCodeSupplier
{
    LOCAL_FILE_NO_FILES(0, EXTERNAL),
    LOCAL_FILE_FILESYSTEM_ERROR(1, EXTERNAL),
    LOCAL_FILE_READ_ERROR(2, EXTERNAL);

    private final ErrorCode errorCode;

    LocalFileErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0501_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
