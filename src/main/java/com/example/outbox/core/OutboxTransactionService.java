package com.example.outbox.core;

import java.time.OffsetDateTime;
import java.util.List;

public interface OutboxTransactionService {

    //** Načte události, které jsou PENDING a zároveň nextAttemptAt <= now. */
    List<OutboxEvent> fetchEventsToProcess(int batchSize, OffsetDateTime now);

    /**
     * Uloží změněný stav události (status, retryCount, nextAttemptAt, errorReason).
     */
    void updateEventState(OutboxEvent event);
}
