package com.example.realtime.repository;

import com.example.realtime.model.UserScore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface UserScoreRepository extends JpaRepository<UserScore , UUID> {

}
