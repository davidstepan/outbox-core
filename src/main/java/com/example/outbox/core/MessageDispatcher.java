package com.example.outbox.core;

public interface MessageDispatcher {

    void dispatch(OutboxEvent event);
}
