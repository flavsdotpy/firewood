package com.ap3x.firewood.models;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileMetadataTest {

    private FileMetadata metadata;

    @Before
    public void setUp() {
        metadata = new FileMetadata();
    }

    @Test
    public void constructorsTest(){
        metadata = new FileMetadata("sourceKey", 1L);

        assertEquals("sourceKey", metadata.getFilePath());
        assertEquals(1L, metadata.getFileExportTimestamp().longValue());
        assertNull(metadata.getResult());
        assertNull(metadata.getExceptionMessage());
        assertNull(metadata.getExecutionTime());
        assertNull(metadata.getOutputRef());
        assertNull(metadata.getSuccessLines());
        assertNull(metadata.getTotalLines());

        metadata = new FileMetadata(
                1L,
                true,
                "sourceKey",
                1L,
                1L,
                1L,
                "exception",
                "output"
        );


        assertEquals(1L, metadata.getExecutionTime().longValue());
        assertTrue(metadata.getResult());
        assertEquals("sourceKey", metadata.getFilePath());
        assertEquals(1L, metadata.getFileExportTimestamp().longValue());
        assertEquals(1L, metadata.getTotalLines().longValue());
        assertEquals(1L, metadata.getSuccessLines().longValue());
        assertEquals("exception", metadata.getExceptionMessage());
        assertEquals("output", metadata.getOutputRef());

    }

    @Test
    public void getSetStatusTest() {
        metadata.setResult(true);

        assertTrue(metadata.getResult());
    }

    @Test
    public void getSetSourceKeyTest() {
        metadata = new FileMetadata("sourceKey", 1L);
        metadata.setFilePath("sourceB");

        assertEquals("sourceB", metadata.getFilePath());
    }

    @Test
    public void getSetSourceTrackingTest() {
        metadata = new FileMetadata("sourceKey", 1L);
        metadata.setFileExportTimestamp(2L);

        assertEquals(2L, metadata.getFileExportTimestamp().longValue());
    }

    @Test
    public void getSetTotalLinesTest() {
        metadata.setTotalLines(1000L);

        assertEquals(1000L, metadata.getTotalLines().longValue());
    }

    @Test
    public void getSetSuccessLinesTest() {
        metadata.setSuccessLines(1000L);

        assertEquals(1000L, metadata.getSuccessLines().longValue());
    }

    @Test
    public void getSetExecutionTimeTest() {
        metadata.setExecutionTime(1000L);

        assertEquals(1000L, metadata.getExecutionTime().longValue());
    }

    @Test
    public void getSetOutputDirTest() {
        metadata.setOutputRef("OutputDir");

        assertEquals("OutputDir", metadata.getOutputRef());
    }

    @Test
    public void getSetExceptionMessageTest() {
        metadata.setExceptionMessage("exceptionMessage");

        assertEquals("exceptionMessage", metadata.getExceptionMessage());
    }


}