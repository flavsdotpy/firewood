package com.ap3x.firewood.helpers;

import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.services.filesource.JsonService;
import com.ap3x.firewood.services.other.S3Service;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

public class FileHelperTest {

    @Mock
    private Environment env;

    @Mock
    private S3Service s3Service;

    @Mock
    private JsonService jsonService;

    @InjectMocks
    private FileHelper helper;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = InvalidParamsException.class)
    public void buildOutputDirThrowsExceptionTest() {
        helper.buildOutputDir(null, "test", "test");
        helper.buildOutputDir("test", null, "test");
        helper.buildOutputDir("test", "test", null);
    }

}
