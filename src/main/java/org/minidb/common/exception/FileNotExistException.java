package org.minidb.common.exception;

public class FileNotExistException extends BaseException {
    public FileNotExistException(){}

    public FileNotExistException(String message){
        super(message);
    }
}
