package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 2.判断优惠券有无开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("优惠券未开始");
        }
        // 3.判断优惠券有无结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("优惠券已过期");
        }
        // 4.判断优惠券是否有库存,因为下面的stock也会判断有库存才减一，所以感觉这里可有可无
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("优惠券库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()){
            //获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }

    }
    @Transactional
    public Result createVoucherOrder(Long voucherId){
        // 查询是否有订单
        int count = voucherOrderService.query()
                .eq("user_id", UserHolder.getUser().getId())
                .eq("voucher_id", voucherId).count();
        if (count >  0){
            return Result.fail("用户已经买过一次了");
        }
        // 5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("优惠券库存不足");
        }
        // 6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //id
        voucherOrder.setId(redisIdWorker.nextId("order"));
        // 优惠券id
        voucherOrder.setVoucherId(voucherId);
        // 用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        // 创建订单
        save(voucherOrder);
        // 7.返回订单
        return Result.ok(voucherOrder);
    }
}
