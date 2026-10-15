package com.example.outbox.core;

import lombok.Data;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

@Data
public class OutboxEvent {

    private String id;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String payload;
    private OffsetDateTime createdAt;
    private String status;
    private int retryCount;
    private OffsetDateTime nextAttemptAt;
    private String errorReason;

    private static final int BASE_DELAY_SECONDS = 2;
    private static final int MAX_RETRY_ATTEMPTS = 5;

    /**
     * Výpočet exponenciálního zpoždění pomocí moderního java.time API.
     */
    public void failAndScheduleRetry(String reason) {
        this.errorReason = reason;
        this.retryCount++;

        if (this.retryCount > MAX_RETRY_ATTEMPTS) {
            this.status = "FAILED";
        } else {
            this.status = "PENDING";
            // Spočítáme sekundy zpoždění: BASE * 2^(retryCount - 1)
            long delaySeconds = BASE_DELAY_SECONDS * (long) Math.pow(2, this.retryCount - 1);

            // Vygenerujeme nový neměnný objekt času posunutý o sekundy dopředu
            this.nextAttemptAt = OffsetDateTime.now().plusSeconds(delaySeconds);
        }
    }

    public void markAsProcessed() {
        this.status = "SENT";
        this.errorReason = null;
    }
}
