package com.ap3x.firewood.helpers;

import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LogHelperTest {

    @InjectMocks
    private LogHelper helper;

    @Before
    public void setUp(){
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void parseLog() {
        String logA = "FATAL 2018-01-01 00:00:01,000";
        String logB = "ERROR 2018/01/01 00:00:01,000";
        String logC = "WARN 2018-01-01";
        String logD = "INFO 2018/01/01";
        String logE = "DEBUG 2018-01-01";
        String logF = "TRACE 2018-01-01";
        String logG = "CUSTOM 2018";

        Row rowA = helper.parseLog(logA);
        Row rowB = helper.parseLog(logB);
        Row rowC = helper.parseLog(logC);
        Row rowD = helper.parseLog(logD);
        Row rowE = helper.parseLog(logE);
        Row rowF = helper.parseLog(logF);
        Row rowG = helper.parseLog(logG);

        assertEquals("FATAL", rowA.getString(0));
        assertEquals("ERROR", rowB.getString(0));
        assertEquals("WARN", rowC.getString(0));
        assertEquals("INFO", rowD.getString(0));
        assertEquals("DEBUG", rowE.getString(0));
        assertEquals("TRACE", rowF.getString(0));
        assertNull(rowG.getString(0));

        assertEquals("2018-01-01 00:00:01,000", rowA.getString(1));
        assertEquals("2018/01/01 00:00:01,000", rowB.getString(1));
        assertEquals("2018-01-01", rowC.getString(1));
        assertEquals("2018/01/01", rowD.getString(1));
        assertEquals("2018-01-01", rowE.getString(1));
        assertEquals("2018-01-01", rowF.getString(1));
        assertNull(rowG.getString(1));
    }
}