package com.ap3x.firewood.helpers;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommonHelperTest {

    @InjectMocks
    private CommonHelper helper;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void isOlapEntityTest() {
        List<String> entities = Arrays.asList("entityA", "entityB");

        assertTrue(helper.isOlapEntity(entities, "entityA"));
        assertFalse(helper.isOlapEntity(entities, "entityC"));
    }

}