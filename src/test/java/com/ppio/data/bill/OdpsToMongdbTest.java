package com.ppio.data.bill;


import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.ppio.data.bill.mongdb.TaskEntity;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SpringBootTest
@Slf4j
@RunWith(SpringRunner.class)
public class OdpsToMongdbTest {

    /**
     * 1. 按天循环  20200901 --  20200931
     * 2. 拿到每天的   distinct  机器  ， 遍历机器 。取出机器的 288 个点
     * 3. 将一个机器的 288 个点 组装成 一条  mong 记录 TaskEntity  , 插入到 mongdb
     */

    @Resource
    MongoTemplate mongoTemplate;

    @Test
    public void test01() throws Exception {


        Account account = new AliyunAccount(Config.ACCESS_ID, Config.ACCESS_KEY);
        Odps odps = new Odps(account);
        odps.setEndpoint(Config.END_POINT);
        odps.setDefaultProject(Config.PROJECT_NAME);
        Instance machineInstance;
        Instance pointsInstance;


        String getDistinctMachineidSql;


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");


        for (int i = 20201001; i <= 20201031; i++) {

            getDistinctMachineidSql = String.format(Config.GET_DISTINCT_MACHINEID_SQL_TEMPLATE, i);

            machineInstance = SQLTask.run(odps, getDistinctMachineidSql);
            machineInstance.waitForSuccess();
            List<Record> machineRecords = SQLTask.getResult(machineInstance);
            log.info("date = {} , machine ids size = {}", i, machineRecords.size());

            String machineId;
            String getMachinePointsSql;

            a:
            for (Record record : machineRecords) {

                machineId = record.getString(0);
                log.info("machine_id  = " + machineId);

                getMachinePointsSql = String.format(Config.get_Machine_Points, i, machineId);
                pointsInstance = SQLTask.run(odps, getMachinePointsSql);
                pointsInstance.waitForSuccess();
                List<Record> pointRecords = SQLTask.getResult(pointsInstance);  //

                if (pointRecords == null || pointRecords.size() == 0) {
                    log.error("机器没有上报点 , dt = {} , machineId = {}", i, machineId);
                    continue a;
                }

                TaskEntity entity = new TaskEntity();
                entity.setTimestamp(Long.valueOf(pointRecords.get(0).getString("time")));
                entity.setDate(dateFormat.format(new Date(1000 * Long.valueOf(pointRecords.get(0).getString("time")))));  //   yyyy-MM-dd
                entity.setDeviceId(machineId);
                entity.setDcacheId(pointRecords.get(0).getString("custom_id"));
//              entity.setBusiness();


                List<Double> series = new ArrayList<>();
                // 3. 将一个机器的 288 个点 组装成 一条  mong 记录 TaskEntity  , 插入到 mongdb  。 漏点  需要补
                //  202009020000    2020 0902 2325
                Record point;
                String time_id;

                int pointIndex = 0;
                for (int j = 0; j < 288; j++) {

                    if (pointIndex >= pointRecords.size()) {
                        series.add(0.0);
                    } else {
                        point = pointRecords.get(pointIndex);
                        time_id = point.getString("time_id");
                        if (Config.POINT_TIMEID_ARR[j].equals(time_id.substring(8, 12))) {
                            pointIndex++;
                            series.add(Long.valueOf(point.getString("bw_upload")) / 1024 / 1024 / 1024.0);
                        } else {
                            series.add(0.0);
                        }
                    }


                }


                entity.setBandwidthSeries(series);

                mongoTemplate.save(entity);
                log.info("success insert one entity = " + entity);


            }

        }


    }


}