package com.ppio.data.bill;

import com.ppio.data.bill.mongdb.TaskEntity;
import com.ppio.data.bill.mysql.entity.TDwsDcacheDockerMonitor;
import com.ppio.data.bill.mysql.mapper.TDwsDcacheDockerMonitorMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
public class MyTask implements Runnable {

    private int i;

    private TDwsDcacheDockerMonitorMapper mapper;

    private MongoTemplate mongoTemplate;

    public MyTask(int i, TDwsDcacheDockerMonitorMapper mapper, MongoTemplate mongoTemplate) {
        this.i = i;
        this.mapper = mapper;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void run() {


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<String> machineRecords = mapper.selectMachines(i);

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

                     //   series.add(point.getBwUpload() / (1024 * 1024 * 1024.0));   // 自己采集的带宽
                        series.add(point.getBsBwUpload() / (1024 * 1024 * 1024.0));   // 爱奇艺日志带宽


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

        log.info("success  finish one day date = " + i);

    }
}
