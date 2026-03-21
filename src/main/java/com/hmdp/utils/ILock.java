package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取锁
     * @paramtimeoutSec 所持有的过期时间，过期后自动释放
     * @return true表示获取锁彻成功 false表示获取锁失败
     *
     */
    boolean tryLock(Long timeoutSec);
    /**
     * 释放锁
     */
    void unLock();
}
