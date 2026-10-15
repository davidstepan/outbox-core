package com.example.outbox.core;

import lombok.RequiredArgsConstructor;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@RequiredArgsConstructor
public class OutboxEventRelay {

    private final OutboxTransactionService transactionService;
    private final MessageDispatcher dispatcher;
    private final Executor executor;
    private final int batchSize;

    public void relayBatch() {

        // Předáváme aktuální čas pro porovnání v DB
        List<OutboxEvent> events = transactionService.fetchEventsToProcess(batchSize, OffsetDateTime.now());

        if (events.isEmpty()) {
            return;
        }

        CompletableFuture<?>[] futures = events.stream()
                .map(event -> CompletableFuture.runAsync(() -> processSingleEvent(event), executor))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private void processSingleEvent(OutboxEvent event) {

        try {

            dispatcher.dispatch(event);

            // Úspěch: Změna stavu v objektu a uložení do DB v samostatné transakci
            event.markAsProcessed();
            transactionService.updateEventState(event);

        } catch (Exception e) {

            // Selhání: Spočítání backoff času uvnitř entity a uložení do DB v samostatné transakci
            event.failAndScheduleRetry(e.getMessage());
            transactionService.updateEventState(event);
        }
    }
}
