package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;

import org.junit.Test;
import java.util.NoSuchElementException;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class CustomOptionalTest {
    @Test(expected = NoSuchElementException.class)
    public void absentThrowsNoSuchElementExceptionOnGet() {
        CustomOptional.absent().get();
    }
}
