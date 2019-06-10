package com.ap3x.firewood.models;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DBOffsetTest {

    private DBOffset offset;

    @Before
    public void setUp() {
        offset = new DBOffset();
    }

    @Test
    public void constructorTest(){
        offset = new DBOffset(
                1L,
                "sourceKey",
                "message"
        );

        assertEquals(1L, offset.getLastExecution().longValue());
        assertEquals("sourceKey", offset.getSourceKey());
        assertEquals("message", offset.getMessage());
    }

    @Test
    public void getLastExecutionTest() {
        offset.setLastExecution(1L);
        assertEquals(1L, offset.getLastExecution().longValue());
    }

    @Test
    public void getSourceKeyTest() {
        offset.setSourceKey("output");
        assertEquals("output", offset.getSourceKey());
    }

    @Test
    public void getMessageTest() {
        offset.setMessage("message");
        assertEquals("message", offset.getMessage());
    }
}