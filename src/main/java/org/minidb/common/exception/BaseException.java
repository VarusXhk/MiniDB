package org.minidb.common.exception;

/**
 * 业务基本异常类
 */
public class BaseException extends RuntimeException{

    public BaseException(){}

    public BaseException(String message){
        super(message);
    }
}
