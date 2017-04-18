package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;

import java.util.NoSuchElementException;

/***
 * Same as {@link Optional}, but throws {@link NoSuchElementException} for missing element.
 */
public abstract class CustomOptional<T> extends Optional<T> {
    public static <T> Optional<T> absent() {
        return AbsentWithNoSuchElement.withType();
    }
}
