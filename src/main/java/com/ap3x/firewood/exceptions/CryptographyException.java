package com.ap3x.firewood.exceptions;

public class CryptographyException extends Exception {
    private static final long serialVersionUID = 662793017805767369L;

	public CryptographyException(Exception e) {
        super(e);
    }
}
