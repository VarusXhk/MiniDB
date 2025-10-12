package org.minidb.common.exception;

public class ConcurrentUpdateException extends BaseException{
    public ConcurrentUpdateException(){}
    public ConcurrentUpdateException(String message){
        super(message);
    }
}
