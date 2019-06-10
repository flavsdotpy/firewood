package com.ap3x.firewood.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class CustomExceptionsTest {

    @Test
    public void cryptographyExceptionTest() {
        CryptographyException exception = new CryptographyException(new Exception("test"));
        assertEquals("java.lang.Exception: test", exception.getMessage());
    }

    @Test
    public void invalidParamsExceptionTest() {
        InvalidParamsException exception = new InvalidParamsException("test");
        assertEquals("test", exception.getMessage());
    }

    @Test
    public void maskExceptionTest() {
        MaskException exception = new MaskException("test");
        assertEquals("test", exception.getMessage());
    }

}