package org.minidb.common.exception;

public class MemoryShortageException extends BaseException {
    public MemoryShortageException() {}
    public MemoryShortageException(String message) {
        super(message);
    }
}
