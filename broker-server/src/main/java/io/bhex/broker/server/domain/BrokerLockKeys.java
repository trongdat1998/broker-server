package io.bhex.broker.server.domain;

public class BrokerLockKeys {

    public static final String INVITE_DAILY_TASK_LOCK_KEY = "invite:daily_task_lock";
    public static final long INVITE_DAILY_TASK_LOCK_EXPRIE = 5 * 60 * 1000;


    public static final String INVITE_TEST_DAILY_TASK_LOCK_KEY = "invite:test_daily_task_lock:%s:%s";
    public static final long INVITE_TEST_DAILY_TASK_LOCK_EXPRIE = 5 * 60 * 1000;

    public static final String POINT_CARD_SCHEDULE_STAGE_KEY = "point_card_schedular_stage:lock";
    public static final long POINT_CARD_SCHEDULE_STAGE_EXPRIE = 30 * 1000;

    public static final String POINT_CARD_SCHEDULE_SPIKE_KEY = "point_card_schedular_spike_sold_out:lock";
    public static final long POINT_CARD_SCHEDULE_SPIKE_EXPRIE = 30 * 1000;

    public static final String TICKET_WELFARE_USER_CARD_LOCK = "ticket_welfare_user_card_lock:";
    public static final long TICKET_WELFARE_USER_CARD_EXPIRE = 30 * 1000;


    public static final String INVITE_UPDATE_VAILD_COUNT_KEY = "invite_update_vaild_count_key:";
    public static final long INVITE_UPDATE_VAILD_COUNT_KEY_EXPIRE = 30 * 1000;

    public static final String REG_STATISTIC_KEY = "reg_statstic_key";
    public static final long REG_STATISTIC_KEY_EXPIRE = 600 * 1000;


    public static final String KYC_STATISTIC_KEY = "kyc_statstic_key";
    public static final long KYC_STATISTIC_KEY_EXPIRE = 600 * 1000;

    public static final String INVITE_GENERATE_RECROD_LOCK_KEY = "invite:generate_record_lock:%s:%s";
    public static final long INVITE_GENERATE_RECROD_LOCK_EXPRIE = 5 * 60 * 1000;

    public static final String INVITE_EXECUTE_RECROD_LOCK_KEY = "invite:execute_record_lock:%s:%s";
    public static final long INVITE_EXECUTE_RECROD_LOCK_EXPRIE = 5 * 60 * 1000;


    public static final String FINANCE_PURCHASE_EXCEPTION_LOCK_KEY = "finance:purchase_exception_lock";
    public static final long FINANCE_PURCHASE_EXCEPTION_LOCK_EXPIRE = 5 * 60 * 1000;

    public static final String FINANCE_REDEEM_TRANSFER_LOCK_KEY = "finance:redeem_transfer_lock";
    public static final long FINANCE_REDEEM_TRANSFER_LOCK_EXPIRE = 5 * 60 * 1000;

    public static final String FINANCE_REDEEM_EXCEPTION_LOCK_KEY = "finance:redeem_exception_lock";
    public static final long FINANCE_REDEEM_EXCEPTION_LOCK_EXPIRE = 5 * 60 * 1000;


    public static final String FINANCE_DAILY_TASK_LOCK_KEY = "finance:daily_task_lock";
    public static final long FINANCE_DAILY_TASK_LOCK_EXPIRE = 60 * 60 * 1000;
    public static final String FINANCE_STOP_FINANCE_LOCK_KEY = "finance:daily_task_lock";
    public static final long FINANCE_STOP_FINANCE_LOCK_LOCK_EXPIRE = 10 * 60 * 1000;


    public static final String AIRDROP_NOTICE_KEY = "airdrop_notice_key";
    public static final long AIRDROP_NOTICE_KEY_EXPIRE = 60 * 1000;

    public static final String AIRDROP_TASK_KEY = "airdrop_task_key_%s_%s";
    public static final long AIRDROP_TASK_KEY_EXPIRE = 10 * 60 * 1000;

    public static final String AIRDROP_EXECUTE_TASK_KEY = "airdrop_execute_task_key_%s_%s";
    public static final long AIRDROP_EXECUTE_TASK_KEY_EXPIRE = 30 * 60 * 1000;

    public static final String ODS_TASK_LOCK_KEY = "ods_lock_key";
    public static final long ODS_TASK_LOCK_KEY_EXPIRE = 300 * 1000;

    public static final String BROKER_TASK_CONFIG_LOCK_KEY = "broker_task_lock_key";
    public static final long BROKER_TASK_CONFIG_LOCK_KEY_EXPIRE = 60 * 1000;

    public static final String USER_LEVER_LOCK_KEY = "user_level_lock_key_%s:%s";
    public static final long USER_LEVER_LOCK_KEY_EXPIRE = 3600_1000;

    public static final String HOBBIT_KPI_STATISTIC_KEY = "hobbit_kpi_statstic_key";
    public static final long HOBBIT_KPI_STATISTIC_KEY_EXPIRE = 310 * 1000;

    public static final String CONVERT_PURCHASE_LOCK_KEY = "convert:purchase_lock";
    public static final long CONVERT_PURCHASE_LOCK_EXPIRE = 5 * 1000;

    public static final String STAKING_CALC_TIME_INTEREST_LOCK_KEY = "staking_calc_time_interest_lock_key_%s";
    public static final long STAKING_CALC_TIME_INTEREST_LOCK_KEY_EXPIRE = 10 * 60 * 1000;

    public static final String UNLOCK_NODE_LOCK_KEY = "vote:unlock_node_lock";
    public static final long UNLOCK_NODE_LOCK_EXPIRE = 30 * 1000;

    public static final String UNFROZEN_VOTE_LOCK_KEY = "vote:unfrozen_vote_lock";
    public static final long UNFROZEN_VOTE_LOCK_EXPIRE = 30 * 1000;
}
