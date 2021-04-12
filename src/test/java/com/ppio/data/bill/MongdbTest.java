package com.ppio.data.bill;

import com.ppio.data.bill.mongdb.DemoDao;
import com.ppio.data.bill.mongdb.DemoEntity;
import com.ppio.data.bill.mongdb.TaskEntity;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;

@SpringBootTest
@Slf4j
@RunWith(SpringRunner.class)
public class MongdbTest {

    @Autowired
    private DemoDao demoDao;


    @Resource
    private MongoTemplate mongoTemplate;

    @Test
    public void test() {

        DemoEntity demoEntity = new DemoEntity();
        demoEntity.setId(1L);
        demoEntity.setTitle("Spring Boot 中使用 MongoDB");
        demoEntity.setDescription("关注公众号，搜云库，专注于开发技术的研究与知识分享");
        demoEntity.setBy("souyunku");
        demoEntity.setUrl("http://www.souyunku.com");

        demoDao.saveDemo(demoEntity);

        demoEntity = new DemoEntity();
        demoEntity.setId(2L);
        demoEntity.setTitle("Spring Boot 中使用 MongoDB");
        demoEntity.setDescription("关注公众号，搜云库，专注于开发技术的研究与知识分享");
        demoEntity.setBy("souyunku");
        demoEntity.setUrl("http://www.souyunku.com");

        demoDao.saveDemo(demoEntity);
    }


    @Test
    public void test2() {
        TaskEntity entity = new TaskEntity();
        entity.setTimestamp(125465555L);
        entity.setDeviceId("1001");
        entity.setDcacheId("200355");
        entity.setDate("2020-12-12");
        entity.setBusiness("daai");
        ArrayList<Double> list = new ArrayList<>();
        list.add(0.00002);
        list.add(0.00000022112);
        list.add(0.0012);
        entity.setBandwidthSeries(list);
        mongoTemplate.save(entity);
    }
}
