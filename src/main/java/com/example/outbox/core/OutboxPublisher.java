package com.example.outbox.core;

public interface OutboxPublisher {

    void publish(OutboxEvent event);
}
