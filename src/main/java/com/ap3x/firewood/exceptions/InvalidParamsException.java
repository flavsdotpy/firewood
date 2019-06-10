package com.ap3x.firewood.exceptions;

public class InvalidParamsException extends RuntimeException {
    private static final long serialVersionUID = 7921890183341798609L;

	public InvalidParamsException(String s) {
        super(s);
    }
}
