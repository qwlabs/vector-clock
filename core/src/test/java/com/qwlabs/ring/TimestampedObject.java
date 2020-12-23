package com.qwlabs.ring;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

public class TimestampedObject<T> {
    private final T value;
    private final LocalDateTime timestamp;

    public TimestampedObject(T value) {
        this(value, null);
    }

    public TimestampedObject(T value, LocalDateTime timestamp) {
        Objects.requireNonNull(value, "value can not be null.");
        this.value = value;
        this.timestamp = Optional.ofNullable(timestamp).orElse(LocalDateTime.now(Clock.systemUTC()));
    }

    public T getValue() {
        return value;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public boolean isAfter(TimestampedObject<?> obj) {
        Objects.requireNonNull(obj, "object can not be null.");
        return obj.getTimestamp().isAfter(obj.getTimestamp());
    }

    @Override
    public String toString() {
        return String.format("{value:%s, timestamp:%s}", value, timestamp);
    }
}
