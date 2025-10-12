package org.minidb.common.exception;

public class DataOverflowException extends BaseException{
    public DataOverflowException(){}
    public DataOverflowException(String message){
        super(message);
    }
}
