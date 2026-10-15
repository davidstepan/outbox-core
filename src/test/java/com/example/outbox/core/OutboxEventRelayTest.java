package com.example.outbox.core;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.Executor;

// Změna importů na JUnit 4
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OutboxEventRelayTest {

    private OutboxTransactionService transactionService;
    private MessageDispatcher dispatcher;
    private OutboxEventRelay relay;

    @Before // V JUnit 4 se používá @Before namísto @BeforeEach
    public void setUp() {
        transactionService = mock(OutboxTransactionService.class);
        dispatcher = mock(MessageDispatcher.class);

        // Synchronní executor pro deterministický běh v jednom vlákně
        Executor sameThreadExecutor = Runnable::run;

        relay = new OutboxEventRelay(transactionService, dispatcher, sameThreadExecutor, 10);
    }

    @Test // org.junit.Test
    public void shouldDoNothingWhenNoEventsFound() {
        when(transactionService.fetchEventsToProcess(eq(10), any(OffsetDateTime.class)))
                .thenReturn(Collections.emptyList());

        relay.relayBatch();

        verifyNoInteractions(dispatcher);
        verify(transactionService, never()).updateEventState(any(OutboxEvent.class));
    }

    @Test
    public void shouldProcessEventSuccessfullyAndMarkAsProcessed() throws Exception {
        OutboxEvent event = new OutboxEvent();

        event.setId("1");
        event.setAggregateType("Order");
        event.setAggregateId("100");
        event.setPayload("OrderCreated");

        when(transactionService.fetchEventsToProcess(eq(10), any(OffsetDateTime.class)))
                .thenReturn(Collections.singletonList(event));

        relay.relayBatch();

        verify(dispatcher).dispatch(event);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(transactionService).updateEventState(captor.capture());

        assertEquals("SENT", captor.getValue().getStatus());
        assertNull(captor.getValue().getErrorReason());
    }

    @Test
    public void shouldCalculateFirstExponentialBackoffOnFailure() throws Exception {
        OutboxEvent event = new OutboxEvent();

        event.setId("1");
        event.setAggregateType("Order");
        event.setAggregateId("100");
        event.setPayload("OrderCreated");
        assertEquals("Nový event musí mít retry count 0", 0, event.getRetryCount());

        when(transactionService.fetchEventsToProcess(eq(10), any(OffsetDateTime.class)))
                .thenReturn(Collections.singletonList(event));

        doThrow(new RuntimeException("Broker is down")).when(dispatcher).dispatch(event);

        OffsetDateTime testStartTime = OffsetDateTime.now();
        relay.relayBatch();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(transactionService).updateEventState(captor.capture());
        OutboxEvent updatedEvent = captor.getValue();

        assertEquals("Po prvním selhání musí zůstat PENDING k opakování", "PENDING", updatedEvent.getStatus());
        assertEquals("Retry count se musí inkrementovat", 1, updatedEvent.getRetryCount());
        assertEquals("Broker is down", updatedEvent.getErrorReason());

        // Backoff: 2s * 2^(1-1) = 2 sekundy
        OffsetDateTime expectedNextAttempt = testStartTime.plus(2, ChronoUnit.SECONDS);
        long driftMillis = Math.abs(ChronoUnit.MILLIS.between(expectedNextAttempt, updatedEvent.getNextAttemptAt()));

        assertTrue("Čas dalšího pokusu se liší o více než 100 ms: " + driftMillis + "ms", driftMillis < 100);
    }

    @Test
    public void shouldEscalateToNextBackoffOnSubsequentFailures() throws Exception {
        OutboxEvent event = new OutboxEvent();

        event.setId("1");
        event.setAggregateType("Order");
        event.setAggregateId("100");
        event.setPayload("OrderCreated");
        event.setRetryCount(2);

        when(transactionService.fetchEventsToProcess(eq(10), any(OffsetDateTime.class)))
                .thenReturn(Collections.singletonList(event));
        doThrow(new RuntimeException("Connection timeout")).when(dispatcher).dispatch(event);

        OffsetDateTime testStartTime = OffsetDateTime.now();
        relay.relayBatch();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(transactionService).updateEventState(captor.capture());
        OutboxEvent updatedEvent = captor.getValue();

        assertEquals("Retry count se musí navýšit na 3", 3, updatedEvent.getRetryCount());

        // 3. pokus: 2s * 2^(3-1) = 8 sekund zpoždění
        OffsetDateTime expectedNextAttempt = testStartTime.plus(8, ChronoUnit.SECONDS);
        long driftMillis = Math.abs(ChronoUnit.MILLIS.between(expectedNextAttempt, updatedEvent.getNextAttemptAt()));

        assertTrue("Čas 3. pokusu (8s backoff) nesouhlasí. Rozdíl: " + driftMillis + "ms", driftMillis < 100);
    }

    @Test
    public void shouldMarkAsHardFailedWhenMaxRetriesExceeded() throws Exception {
        OutboxEvent event = new OutboxEvent();

        event.setId("1");
        event.setAggregateType("Order");
        event.setAggregateId("100");
        event.setPayload("OrderCreated");
        event.setRetryCount(5);

        when(transactionService.fetchEventsToProcess(eq(10), any(OffsetDateTime.class)))
                .thenReturn(Collections.singletonList(event));
        doThrow(new RuntimeException("Fatal error")).when(dispatcher).dispatch(event);

        relay.relayBatch();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(transactionService).updateEventState(captor.capture());
        OutboxEvent updatedEvent = captor.getValue();

        assertEquals("Zpráva musí přejít do stavu FAILED", "FAILED", updatedEvent.getStatus());
        assertEquals(6, updatedEvent.getRetryCount());
        assertEquals("Fatal error", updatedEvent.getErrorReason());
    }
}
