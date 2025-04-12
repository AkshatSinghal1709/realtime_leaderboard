package com.example.realtime.consumers;

import com.example.realtime.dto.LeaderboardDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LeaderboardConsumer {
    private static final Logger log = LoggerFactory.getLogger(LeaderboardConsumer.class);

    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;

    @KafkaListener(topics = "leaderboard", groupId = "leaderboard_consumer", containerFactory = "leaderboardKafkaListenerContainerFactory")
    public void consumeLeaderboard(ConsumerRecord<String , LeaderboardDto> record) {
        var leaderboardDto = record.value();

        log.info("record consumed with timestampMs: {}", leaderboardDto.getLastModifyTimestamp());
        simpMessagingTemplate.convertAndSend("/live_updates/leaderboard", leaderboardDto);
        log.info("record sent to websocket topic");
    }
}
