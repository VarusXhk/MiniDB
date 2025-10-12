package org.minidb.common.exception;

public class DatabaseBusyException extends BaseException{
    public DatabaseBusyException(){}
    public DatabaseBusyException(String message) {
        super(message);
    }
}
