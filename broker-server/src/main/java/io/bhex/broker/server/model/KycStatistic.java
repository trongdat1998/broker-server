package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @Description:
 * @Date: 2018/12/13 下午3:04
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Table(name = "tb_kyc_statistic")
public class KycStatistic {
    @Id
    private Long id;
    private Long orgId;
    //0-明细 1-汇总
    private Integer aggregate;
    //统计日期 yyyy-MM-dd
    private String statisticDate;
    private Long passedKycNumber = 0L;
    private Long pcPassedKycNumber = 0L;
    private Long androidPassedKycNumber = 0L;
    private Long iosPassedKycNumber = 0L;
    private Long rejectKycNumber = 0L;
    private Long pcRejectKycNumber = 0L;
    private Long androidRejectKycNumber = 0L;
    private Long iosRejectKycNumber = 0L;
    private Long lastVerifyHistoryId = 0L;
    private java.sql.Timestamp created;
    private java.sql.Timestamp updated;

    public static KycStatistic defaultAggregateInstance(Long orgId) {
        KycStatistic regStatistic = new KycStatistic();
        regStatistic.setOrgId(orgId);
        regStatistic.setCreated(new Timestamp(System.currentTimeMillis()));
        regStatistic.setUpdated(new Timestamp(System.currentTimeMillis()));
        regStatistic.setLastVerifyHistoryId(0L);
        regStatistic.setAggregate(1);
        regStatistic.setStatisticDate("total");
        return regStatistic;
    }

    public void incrPassedKycNumber(Integer incrBy) {
        passedKycNumber = passedKycNumber + incrBy;
    }

    public void incrPcPassedKycNumber(Integer incrBy) {
        pcPassedKycNumber = pcPassedKycNumber + incrBy;
    }

    public void incrAndroidPassedKycNumber(Integer incrBy) {
        androidPassedKycNumber = androidPassedKycNumber + incrBy;
    }

    public void incrIosPassedKycNumber(Integer incrBy) {
        iosPassedKycNumber = iosPassedKycNumber + incrBy;
    }

    public void incrRejectKycNumber(Integer incrBy) {
        rejectKycNumber = rejectKycNumber + incrBy;
    }

    public void incrPcRejectKycNumber(Integer incrBy) {
        pcRejectKycNumber = pcRejectKycNumber + incrBy;
    }

    public void incrAndroidRejectKycNumber(Integer incrBy) {
        androidRejectKycNumber = androidRejectKycNumber + incrBy;
    }

    public void incrIosRejectKycNumber(Integer incrBy) {
        iosRejectKycNumber = iosRejectKycNumber + incrBy;
    }


//    public static void main(String[] args) {
////        Field[] fields = KycStatistic.class.getDeclaredFields();
////        for (Field field : fields){
////            String name = field.getName();
////            String content = "public void incr" + name.substring(0,1).toUpperCase() + name.substring(1) +"(Integer incrBy){\n  "+name+" = "+name+" + incrBy;\n}";
////
////        }
//
////        Field[] fields = KycStatistic.class.getDeclaredFields();
////        for (Field field : fields){
////            String name = field.getName();
////            String upperName =  name.substring(0,1).toUpperCase() + name.substring(1);
////            StringBuffer content = new StringBuffer();
////            content.append("s.set").append(upperName).append("(orginalStat.get").append(upperName).append("() + incrStat.get").append(upperName).append("());");
////        }
//
//       // int64 broker_id = 4;
//
//
//
//
//
//        //s1.setInviteRegNumber(s1.getInviteRegNumber() + s2.getInviteRegNumber());
//
//        Field[] fields = KycStatistic.class.getDeclaredFields();
//        StringBuffer content = new StringBuffer();
//        int count = 0;
//        for (Field field : fields){
//            String name = field.getName();
//
//
//            String sqlname = "";
//            char[] chars = name.toCharArray();
//            for (int i = 0, length = chars.length; i < length; i++) {
//                char c = chars[i];
//                if (Character.isUpperCase(c)){
//                    chars[i] = Character.toLowerCase(c);
//                    sqlname += "_" + chars[i];
//                }
//                else {
//                    sqlname += chars[i];
//                }
//            }
//            content.append("int64 ").append(sqlname).append("=").append(++count).append(";\n");
//
//
//        }
//        System.err.println(content.toString());
//    }
}
