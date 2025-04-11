package com.example.realtime.consumers;

import com.example.realtime.dto.GameScoreDto;
import com.example.realtime.model.UserDetail;
import com.example.realtime.model.UserScore;
import com.example.realtime.repository.UserDetailRepository;
import com.example.realtime.repository.UserScoreRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GameScoreConsumer {

    private static final Logger log = LoggerFactory.getLogger(GameScoreConsumer.class);
    @Autowired
    private UserScoreRepository userScoreRepository;

    @Autowired
    private UserDetailRepository userDetailRepository;

   @KafkaListener(topics = "game-scores", groupId = "leaderboard-group", containerFactory = "gameScoreKafkaListenerContainerFactory")
   public void consumeGameScore(ConsumerRecord<String , GameScoreDto> record) {
        var gameScoreDto = record.value();
        log.info("record consumed in gameScore consumer , key: {}, value: {}", record.key() , record.value());
        System.out.println("Consumed Message :" + gameScoreDto.toString());

        UserScore userScore = new UserScore();
        UserDetail user = userDetailRepository.findByUsername(gameScoreDto.getUsername()).orElse(null);

        userScore.setUser(user);
        userScore.setScore(gameScoreDto.getScore());
        userScore.setTimestamp(gameScoreDto.getCreatedAt());

        userScoreRepository.save(userScore);
        log.info("record saved to db , key: {}", record.key());
        log.info("exiting gameScore consumer!!!");
    }
}
