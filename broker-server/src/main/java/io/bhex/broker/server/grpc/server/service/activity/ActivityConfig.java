package io.bhex.broker.server.grpc.server.service.activity;

public class ActivityConfig {

    public static final String GUILD_TOPIC = "guild";

    public static final String CARD_GTP_TOPIC = "card-gtp";

    public static final String GUILD_REGISTER_TAG = "guild-register";
    public static final String GUILD_REGISTER_REMOVE_TAG = "guild-register-remove";
    public static final String GUILD_DEPOSIT_TAG = "guild-deposit";

    public static final String GUILD_REGISTER_KEY = "guild-register-key-";
    public static final String GUILD_REGISTER_REMOVE_KEY = "guild-register-key-";
    public static final String GUILD_DEPOSIT_KEY = "guild-deposit-key-";

    public static final String GUILD_ACCUMULATE_KEY = "guild-accumulate-key-";
    public static final String GUILD_EXPIRE_CARD_KEY = "guild-expire-card-key";
    public static final String GUILD_UNLOCK_DOYEN_CARD_KEY = "guild-unlock-doyen-card-key-";

    public static final String GUILD_UNLOCK_NEW_HARD_CARD_KEY = "guild-unlock-new-hard-card-key-";

    public static final String SEND_DOYENCARD_TAG = "send-doyenCard-tag";
    public static final String UNLOCK_DOYENCARD_TAG = "unlock-doyenCard-tag";
    public static final String SEND_WELFARE_TAG = "send-welfareCard-tag";
    public static final String UNLOCK_WELFARE_CARD = "unlock-welfareCard-tag";
    public static final String UNLOCK_NEWHAND_CARD = "unlock-newhand-tag";
    public static final byte KYC = 4;

    public static class CardType {
        public static final byte NEW_HAND_CARD = 1;
        public static final byte DOYEN_CARD = 2;
        public static final byte WELFARE_CARD = 3;
    }

    //券状态，0=无效，1=未使用，2=已使用，3=过期
    public static class UserCardStatus {
        public static final byte INVALID = 0;
        public static final byte VALID = 1;
        public static final byte USED = 2;
        public static final byte EXPIRE = 3;

    }

    public enum GtpType {

        /**
         * 解锁新手卡
         */
        UNLOCK_NEW_HAND_CARD(10),

        /**
         * 解锁达人卡
         */
        UNLOCK_DOYEN_CARD(11),

        /**
         * 解锁普照卡
         */
        UNLOCK_WELFARE_CARD(12),

        /**
         * KYC认证
         */
        KYC(13);


        private int type;

        GtpType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }
}
