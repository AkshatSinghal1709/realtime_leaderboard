package com.example.realtime.producers;

import com.example.realtime.consumers.GameScoreConsumer;
import com.example.realtime.dto.GameScoreDto;
import com.example.realtime.model.UserDetail;
import com.example.realtime.repository.UserDetailRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class GameScoreProducer {

    private static final Logger log = LoggerFactory.getLogger(GameScoreProducer.class);
    @Autowired
    private KafkaTemplate<String , GameScoreDto> kafkaGameScoreTemplate;

    @Autowired
    private UserDetailRepository userDetailRepository;

    @Scheduled(fixedRate = 5000)
    public void randomGameScores(){
        int batchSize = 10;
        for (int i = 1; i <= batchSize; i++) {
            String username = "user" + i;
            int score = (int) (Math.random() * 100);
            GameScoreDto gameScoreDto = new GameScoreDto();

            UserDetail user = userDetailRepository.findByUsername(username).orElse(null);
            UUID userId = user != null ? user.getId() : UUID.randomUUID();
            gameScoreDto.setUserId(userId);
            gameScoreDto.setUsername(username);
            gameScoreDto.setScore(score);
            gameScoreDto.setCreatedAt(LocalDateTime.now());

            kafkaGameScoreTemplate.send("game-scores", username , gameScoreDto);
            log.info("Produced message: {}", gameScoreDto);
        }
    }
}
