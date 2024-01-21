package io.bhex.broker.server.domain;

public class AirdropStatus {

    public static Integer STATUS_INIT = 10;
    public static Integer STATUS_SUCCESS = 1; //空投成功
    public static Integer STATUS_AIRDROP = 2; //空投中
    public static Integer STATUS_FAILED = 3; //空投失败
    public static Integer STATUS_PART_SUCCESS = 4; //空投部分成功
    public static Integer STATUS_CLOSED = 5;
    public static Integer STATUS_AIRDROP_TRANSFER_WAITING = 10; //空投记录等待执行中(空投中-扩展状态)
    public static Integer STATUS_AIRDROP_TRANSFER_EXECUTING = 11; //空投记录任务执行中(空投中-扩展状态)
    public static Integer STATUS_FAILED_INSUFFICIENT = 31;

    public static final Integer STATUS_AUDIT_PASSED = 14; //审核通过，可进行空投
    public static final Integer STATUS_AUDIT_REJECTED = 15; //审核拒绝，不可进行空投

}
