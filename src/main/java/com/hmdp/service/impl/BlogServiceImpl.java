package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {


    @Resource
    private UserServiceImpl userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    //没点进去查询
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    //判断是否点赞
    private void isBlogLiked(Blog blog) {
        //获取用户ID
        Long userId = UserHolder.getUser().getId();
        //获取博客ID
        Long blogId = blog.getId();
        //获取key
        String key = BLOG_LIKED_KEY + blogId;
        //利用以上两个ID判断redis中是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        //设置blog对象中的islike字段
        blog.setIsLike(score != null);
    }
    //点进某个博客查询
    @Override
    public Result queryBlogById(Long id) {
        // 1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 2.查询并封装用户信息进Blog类
        queryBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }
    //点赞
    @Override
    public Result likeBlog(Long id) {
        //获取用户Id 排除还没有登录的情况
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return Result.fail("还未登录");
        }
        Long userId = user.getId();
        //获取key
        String key = BLOG_LIKED_KEY + id;
        //判断redis中对应blogID（键）有没有被当前用户ID（值）点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //如果没有，则在这个键中添加值，并修改数据库中该blog的点赞数量+1
        if (score == null){
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else {//如果有，则在这个键中删减对应userId，并修改数据库中该blog的点赞数量-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }

        }

        return Result.ok();


    }
    //利用blogid得userid集合进而得userDTO集合  获取前五个点赞的人
    @Override
    public Result queryBlogLikes(Long id) {
        //获取key
        String key = BLOG_LIKED_KEY + id;
        //获取userId集合
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //获取userDTO集合
        String idStr = StrUtil.join(",", ids);
        // 3.根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 4.返回
        return Result.ok(userDTOS);


    }
    //发送blog
    @Override
    public Result saveBlog(Blog blog) {
        //获取用户ID，赋值给blog,并存进数据库
        Long userId = UserHolder.getUser().getId();
        blog.setId(userId);
        boolean isSuccess = save(blog);
        //判断是否成功
        if (!isSuccess){
            //失败
            return Result.fail("未存入数据库");
        }
        //成功：利用db与userid获取他对应的粉丝ID集合
        List<Follow> follows = followService.query().eq("follow_user_id", userId).list();
        //以推的方式存入redis key是followid，值是blog
        for (Follow follow:follows){
            //粉丝ID
            Long id = follow.getUserId();
            String key = "feed:" + id;
            //存入redis
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());

        }
        return Result.ok(blog.getId());
    }
    //当前用户查询自己所关注的人发来的blog
    @Override
    public Result queryBlogOfFollow(Long max, Integer offest) {
        //利用五个参数去redis中查询followid-blogidlist键值对
        //1.获取当前用户userid并生成key
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
        //2.查redis  会返回一个封装blogidlist,时间戳list的集合
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores
                (key, 0, max, offest, 2);
        //判断是否非空
        if (typedTuples == null || typedTuples.isEmpty()){
            //空
            return Result.ok("暂无推送博客");
        }

        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        //遍历这个集合的所有对象（每个对象有redis中存的value和score两个属性，
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //每一次遍历doing：将对象的value（long）装到ids集合里，将score结合算法计算出下次执行这个接口要传的max与offest
            ids.add(Long.valueOf(typedTuple.getValue()));//这里需要创建一个ids集合接收
            long time = typedTuple.getScore().longValue();
            if (time == minTime){
                os++;
            } else {
                minTime = time;
                os = 1;
            }

        }

        //利用blogidlist查询bloglist(记得加上点赞状态）
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Blog blog : blogList) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }
        //最终将bloglist,max,offest封装到一个对象中
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(offest);
        return Result.ok(scrollResult);
    }
    //查询该博客的用户
    public void queryBlogUser(Blog blog){
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}

















