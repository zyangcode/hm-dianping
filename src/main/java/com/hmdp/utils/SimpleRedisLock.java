package com.hmdp.utils;

public class SimpleRedisLock implements ILock{

    @Override
    public boolean tryLock(Long timeoutSec) {
        return false;
    }

    @Override
    public void unLock() {

    }
}
