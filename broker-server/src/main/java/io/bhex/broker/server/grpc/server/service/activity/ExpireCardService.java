package io.bhex.broker.server.grpc.server.service.activity;

import com.google.common.collect.Lists;
import io.bhex.broker.server.model.ActivityUserCard;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

@Service
public class ExpireCardService {

    @Resource
    private ActivityRepositoryService activityRepositoryService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;


    public void expire() {

        Timestamp now = Timestamp.from(new Date().toInstant());

        Example exp = new Example(ActivityUserCard.class);
        exp.createCriteria()
                .andEqualTo("cardStatus", (byte) 1)
                .andLessThanOrEqualTo("expireTime", now);

        List<ActivityUserCard> cards = activityRepositoryService.listUserCard(exp);
        if (CollectionUtils.isEmpty(cards)) {
            return;
        }

        List<List<ActivityUserCard>> partList = Lists.partition(cards, 10);
        partList.forEach(list -> {
            taskExecutor.execute(() -> {
                list.forEach(card -> {
                    card.expire();
                    card.setUpdateTime(now);
                });

                activityRepositoryService.updateUserCardBatch(list);
            });
        });


    }
}
