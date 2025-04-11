package com.example.realtime.consumers;

import com.example.realtime.dto.LeaderboardChangeDto;
import com.example.realtime.dto.LeaderboardDto;
import com.example.realtime.dto.UserDto;
import com.example.realtime.model.UserDetail;
import com.example.realtime.repository.UserDetailRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Slf4j
@Component
public class LeaderboardChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(LeaderboardChangeConsumer.class);
    @Value("${spring.redis.leaderboard-cache-key}")
    private String leaderboardCacheKey;

    @Value("${spring.redis.leaderboard-key}")
    private String leaderboardKey;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisTemplate<String , UserDto> userDtoRedisTemplate;

    @Autowired
    private UserDetailRepository userDetailRepository;

    @Autowired
    private RedisTemplate<String , LeaderboardDto> leaderboardDtoRedisTemplate;

    @Autowired
    private KafkaTemplate<String , LeaderboardDto> kafkaLeaderboardTemplate;


    @KafkaListener(topics = "leaderboard-changes", groupId = "leaderboard-group", containerFactory = "leaderboardKafkaListenerContainerFactory")
    public void consumeLeaderboardChange(ConsumerRecord<String , LeaderboardChangeDto> record) {

        var leaderboardChange = record.value();

        log.info("record consumed with timestampMs: {}", leaderboardChange.getRecordTimestampMs());

        var leaderboardCache = leaderboardDtoRedisTemplate.opsForValue().get(leaderboardCacheKey);
        long currentTimeMs = System.currentTimeMillis();

        if(!canRefreshLeaderboard(leaderboardCache, currentTimeMs)) {
            log.info("Skipping leaderboard refresh as the cache is still valid. , currentTs: {} , cacheTs: {}", currentTimeMs, leaderboardCache!=null?leaderboardCache.getLastModifyTimestamp():null);
            return;
        }

        Set<ZSetOperations.TypedTuple<String>> leaderboard = stringRedisTemplate.opsForZSet().reverseRangeWithScores(leaderboardKey, 0, 9);

        if(Objects.isNull(leaderboard)){
            log.info("No leaderboard data found in Redis");
            return;
        }

        LeaderboardDto leaderboardDto = maptoLeaderboardDto(leaderboard , currentTimeMs);

        leaderboardDtoRedisTemplate.opsForValue().set(leaderboardCacheKey , leaderboardDto);
        kafkaLeaderboardTemplate.send("leaderboard", leaderboardDto);
        log.info("Sent leaderboard data to Kafka topic: {}", "leaderboard-changes");
    }

    private LeaderboardDto maptoLeaderboardDto(Set<ZSetOperations.TypedTuple<String>> leaderboard, long currentTimeMs) {
        int rank = 0;
        List<LeaderboardDto.User> userList = new ArrayList<>(leaderboard.size());
        for (ZSetOperations.TypedTuple<String> tuple : leaderboard) {
            rank++;
            String username = tuple.getValue();
            Double score = tuple.getScore();

            UserDto userDto = getCachedUserDto(username);
            LeaderboardDto.User user = new LeaderboardDto.User();
            user.setRank(rank);
            user.setScore(tuple.getScore());
            user.setNickname(userDto.getNickname());
            user.setUserId(userDto.getUserId());
            user.setUsername(userDto.getUsername());
            userList.add(user);
        }

        LeaderboardDto leaderboardDto = new LeaderboardDto();
        leaderboardDto.setUsers(userList);
        leaderboardDto.setLastModifyTimestamp(currentTimeMs);
        log.info("LeaderboardDto: {}", leaderboardDto);

        return leaderboardDto;
    }

    private UserDto getCachedUserDto(String username) {
        UserDto userDto = userDtoRedisTemplate.opsForValue().get(username);
        if (userDto == null) {
            UserDetail user = userDetailRepository.findByUsername(username).orElse(null);

            userDto = new UserDto();
            userDto.setUserId(user.getId());
            userDto.setUsername(user.getUsername());
            userDto.setNickname(user.getNickname());
            userDtoRedisTemplate.opsForValue().set(username, userDto);
        }
        log.info("UserDto for username {}: {}", username, userDto);
        return userDto;
    }

    private boolean canRefreshLeaderboard(LeaderboardDto leaderboardCache, long currentTimeMs) {
        long throttleThreshold = currentTimeMs - 1000;

        return Objects.isNull(leaderboardCache) || leaderboardCache.getLastModifyTimestamp() < throttleThreshold;
    }
}
