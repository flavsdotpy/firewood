package com.ap3x.firewood.common;

import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

public class FaioConfigurationTest {

    @InjectMocks
    private FirewoodConfiguration firewoodConfiguration;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }
}