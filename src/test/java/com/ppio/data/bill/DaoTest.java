package com.ppio.data.bill;

import com.ppio.data.bill.mysql.entity.TDwsDcacheDockerMonitor;
import com.ppio.data.bill.mysql.mapper.TDwsDcacheDockerMonitorMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;


@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class DaoTest {

    @Autowired
    TDwsDcacheDockerMonitorMapper mapper;


    @Qualifier("asyncServiceExecutor")
    @Autowired
    Executor executor;

    // fb477aef6f9b4e5c8fc784a169e3eabf
    @Test
    public void test() {

        // fb477aef6f9b4e5c8fc784a169e3eabf

//        List<String> machines = mapper.selectMachines(20200901);
//        System.out.println("机器数量  = " + machines.size());
//        for (String machine : machines) {
//            System.out.println(machine);
//        }

        // 一个机器的   所有点
        List<TDwsDcacheDockerMonitor> monitors = mapper.selectByMachineId(20200901, "fb477aef6f9b4e5c8fc784a169e3eabf");

        System.out.println("points size =  " + monitors.size());
        for (TDwsDcacheDockerMonitor monitor : monitors) {
            log.info(monitor.getTimeId() + " , " + monitor.getBwUpload());
        }

    }


    /**
     * 1. 按天循环  20200901 --  20200931
     * 2. 拿到每天的   distinct  机器  ， 遍历机器 。取出机器的 288 个点
     * 3. 将一个机器的 288 个点 组装成 一条  mong 记录 TaskEntity  , 插入到 mongdb
     */

    @Resource
    MongoTemplate mongoTemplate;

    @Test
    public void test01() throws Exception {


//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        for (int i = 20210301; i <= 20210331; i++) {

            executor.execute(new MyTask(i, mapper, mongoTemplate));

           /* List<String> machineRecords = mapper.selectMachines(i);

            log.info(" start  date = {} , machine ids size = {}", i, machineRecords.size());

            a:
            for (String mid : machineRecords) {


                List<TDwsDcacheDockerMonitor> pointRecords = mapper.selectByMachineId(i, mid);

                if (pointRecords == null || pointRecords.size() == 0) {
                    log.error("机器没有上报点 , dt = {} , machineId = {}", i, mid);
                    continue a;
                }

                TaskEntity entity = new TaskEntity();
                entity.setTimestamp(pointRecords.get(0).getTime());
                entity.setDate(dateFormat.format(new Date(1000 * pointRecords.get(0).getTime())));  //   yyyy-MM-dd
                entity.setDeviceId(mid);
                entity.setDcacheId(pointRecords.get(0).getCustomId());
//              entity.setBusiness();


//                List<BwEntity> series = new ArrayList<>();
                List<Double> series = new ArrayList<>();
                // 3. 将一个机器的 288 个点 组装成 一条  mong 记录 TaskEntity  , 插入到 mongdb  。 漏点  需要补
                //  202009020000    2020 0902 2325
                TDwsDcacheDockerMonitor point;
                String time_id;

                int pointIndex = 0;
                for (int j = 0; j < 288; j++) {

                    if (pointIndex >= pointRecords.size()) {
                        series.add(0.0);
//                        series.add(new BwEntity(Config.POINT_TIMEID_ARR[j], 0.0));
                    } else {
                        point = pointRecords.get(pointIndex);
                        time_id = point.getTimeId();
                        if (Config.POINT_TIMEID_ARR[j].equals(time_id.substring(8, 12))) {
                            pointIndex++;
//                            series.add(new BwEntity(time_id, point.getBwUpload() / 1024 / 1024 / 1024.0));
                            series.add(point.getBwUpload() / 1024 / 1024 / 1024.0);
                        } else {
                            series.add(0.0);
//                            series.add(new BwEntity(Config.POINT_TIMEID_ARR[j], 0.0));
                        }
                    }
                }

                entity.setBandwidthSeries(series);
//                entity.setSeries(series);

                mongoTemplate.save(entity);
//                log.info("insert one entity = " + entity);


            }

            log.info("success  finish one day date = " + i);*/
        }

        Thread.sleep(99000000L);
    }

}
