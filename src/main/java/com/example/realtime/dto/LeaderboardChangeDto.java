package com.example.realtime.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LeaderboardChangeDto implements Serializable {
    private LocalDateTime recordTimestampMs;

    public LocalDateTime getRecordTimestampMs() {
        return recordTimestampMs;
    }

    public void setRecordTimestampMs(LocalDateTime recordTimestampMs) {
        this.recordTimestampMs = recordTimestampMs;
    }
}
