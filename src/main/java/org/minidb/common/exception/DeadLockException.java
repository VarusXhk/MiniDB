package org.minidb.common.exception;

public class DeadLockException extends BaseException{
    public DeadLockException(){}
    public DeadLockException(String message){
        super(message);
    }
}
