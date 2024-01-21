package io.bhex.broker.server.grpc.server.service.activity;

import io.bhex.broker.server.model.ActivityCard;
import io.bhex.broker.server.model.ActivityUserCard;
import lombok.Data;

import java.util.List;

@Data
public class MessageBody {

    @Data
    public static class DoyenCard {
        private Long userId;

        private List<Long> followerIds;
    }

    @Data
    public static class WelfareCard {
        private Long groupId;

        private List<Long> memberIds;
    }

    @Data
    public static class UnlockDoyenCard {

        private Long userId;

        private List<Long> followerIds;

        private List<ActivityUserCard> userCards;

    }

    @Data
    public static class UnlockWelfareCard {

        private ActivityCard activityCard;

        private ActivityUserCard userCard;
    }

    @Data
    public static class UnlockNewHandCard {
        private ActivityUserCard userCards;
    }


    @Data
    public static class UnlockGTPMessage {

        private Integer type;

        private Long userId;
    }
}
