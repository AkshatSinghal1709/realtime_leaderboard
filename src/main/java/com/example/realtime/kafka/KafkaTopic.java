package com.example.realtime.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopic {

    @Bean
    public NewTopic gameScoreTopic() {
        return TopicBuilder.name("game_score")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic leaderboardChangeTopic() {
        return TopicBuilder.name("leaderboard-changes")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic leaderboardTopic() {
        return TopicBuilder.name("leaderboard")
                .partitions(3)
                .replicas(1)
                .build();
    }
}

