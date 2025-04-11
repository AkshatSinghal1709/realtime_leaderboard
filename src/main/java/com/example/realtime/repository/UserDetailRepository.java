package com.example.realtime.repository;

import com.example.realtime.model.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;


@Repository
public interface UserDetailRepository extends JpaRepository<UserDetail, UUID> {
    Optional<UserDetail> findByUsername(String username);
}