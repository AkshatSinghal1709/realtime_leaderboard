package com.example.realtime.consumers;

import com.example.realtime.dto.GameScoreDto;
import com.example.realtime.dto.LeaderboardChangeDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class RedisConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisConsumer.class);
    @Value("${spring.redis.leaderboard-key}")
    private String leaderboardKey;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private KafkaTemplate<String , LeaderboardChangeDto> kafkaLeaderboardChangeTemplate;

    @KafkaListener(topics = "game-scores", groupId = "leaderboard-change", containerFactory = "gameScoreKafkaListenerContainerFactory")
    public void consumeRedisGameScore(ConsumerRecord<String , GameScoreDto> record) {
      var gameScoreDto = record.value();
      var score = gameScoreDto.getScore();

      log.info("record consumed in redis consumer , userId: {}, score: {} ", gameScoreDto.getUserId(), score);
      stringRedisTemplate.opsForZSet().incrementScore(leaderboardKey , gameScoreDto.getUsername(), score);

      log.info("record with userId: {} , score : {} added to redis set", gameScoreDto.getUserId(), score);

      LeaderboardChangeDto leaderboardChangeDto = new LeaderboardChangeDto();
      leaderboardChangeDto.setRecordTimestampMs(LocalDateTime.now());
      kafkaLeaderboardChangeTemplate.send("leaderboard-changes", leaderboardChangeDto);
      log.info("record with userId: {} , score : {} sent to kafka topic", gameScoreDto.getUserId(), score);
      log.info("exiting redis consumer!!!!");
    }
}
