package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;


import com.google.cloud.dataflow.sdk.repackaged.com.google.common.annotations.GwtCompatible;

import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/***
 * Same as {@link Optional}, but throws {@link NoSuchElementException} for missing element.
 */
@GwtCompatible
public class AbsentWithNoSuchElement<T> extends Optional<T> {
    private static final AbsentWithNoSuchElement INSTANCE = new
            AbsentWithNoSuchElement();
    private static final long serialVersionUID = 0L;

    public static <T> Optional<T> withType() {
        return INSTANCE;
    }

    @Override
    public boolean isPresent() {
        return Optional.<T>absent().isPresent();
    }

    @Override
    public T get() {
        throw new NoSuchElementException();
    }

    @Override
    public T or(T t) {
        return Optional.<T>absent().or(t);
    }

    @Override
    public Optional<T> or(Optional<? extends T> optional) {
        return Optional.<T>absent().or(optional);
    }

    @Override
    public T or(Supplier<? extends T> supplier) {
        return Optional.<T>absent().or(supplier);
    }

    @Nullable
    @Override
    public T orNull() {
        return Optional.<T>absent().orNull();
    }

    @Override
    public Set<T> asSet() {
        return Optional.<T>absent().asSet();
    }

    @Override
    public <V> Optional<V> transform(Function<? super T, V> function) {
        return Optional.<T>absent().transform(function);
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return Optional.<T>absent().hashCode();
    }

    @Override
    public String toString() {
        return Optional.<T>absent().toString();
    }
}
