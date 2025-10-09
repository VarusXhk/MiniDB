package org.minidb.common.exception;

public class FileExistsException extends BaseException{
    public FileExistsException() {}

    public FileExistsException(String message) {
        super(message);
    }
}
