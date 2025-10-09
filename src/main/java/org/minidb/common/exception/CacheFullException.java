package org.minidb.common.exception;

public class CacheFullException extends BaseException{
    public CacheFullException() {}
    public CacheFullException(String message) {
        super(message);
    }
}
