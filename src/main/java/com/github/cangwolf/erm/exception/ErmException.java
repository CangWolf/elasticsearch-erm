package com.github.cangwolf.erm.exception;

/**
 * Created by roy on 16-3-9.
 */
public class ErmException extends RuntimeException{

    private String errType;

    public ErmException(String message, Throwable cause, String errType) {
        super(message, cause);
        this.errType = errType;
    }

    public ErmException(String errType, String message) {
        super(message);
        this.errType = errType;
    }
}
