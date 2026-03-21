package com.hmdp.utils;

import java.time.LocalDateTime;

public class LogicalExpire<T> {

    private LocalDateTime expireTime;
    private T data;
    public LogicalExpire() {
    }

    public LogicalExpire(LocalDateTime expireTime, T data) {
        this.expireTime = expireTime;
        this.data = data;
    }

    public T getData() {
        return data;
    }

    public LocalDateTime getExpireTime() {
        return expireTime;
    }
}
