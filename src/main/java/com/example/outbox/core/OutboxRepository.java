package com.example.outbox.core;

/*
@Update("UPDATE outbox_table SET " +
        "status = #{status}, " +
        "retry_count = #{retryCount}, " +
        "next_attempt_at = #{nextAttemptAt}, " +
        "error_reason = #{errorReason} " +
        "WHERE id = #{id}")
void updateEventState(OutboxEvent event);
 */
public interface OutboxRepository {

    void save(OutboxEvent event);

    OutboxEvent findById(String eventId);
}
