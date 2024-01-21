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
@Table(name = "tb_reg_statistic")
public class RegStatistic {
    @Id
    private Long id;
    private Long orgId;
    //0-明细 1-汇总
    private Integer aggregate;
    //统计日期 yyyy-MM-dd
    private String statisticDate;
    private Long regNumber = 0L;
    private Long pcRegNumber = 0L;
    private Long androidRegNumber = 0L;
    private Long iosRegNumber = 0L;
    private Long notInvitedRegNumber = 0L;
    private Long pcNotInvitedRegNumber = 0L;
    private Long androidNotInvitedRegNumber = 0L;
    private Long iosNotInvitedRegNumber = 0L;
    private Long inviteRegNumber = 0L;
    private Long pcInviteRegNumber = 0L;
    private Long androidInviteRegNumber = 0L;
    private Long iosInviteRegNumber = 0L;
    private Long directInviteRegNumber = 0L;
    private Long pcDirectInviteRegNumber = 0L;
    private Long androidDirectInviteRegNumber = 0L;
    private Long iosDirectInviteRegNumber = 0L;
    private Long indirectInviteRegNumber = 0L;
    private Long pcIndirectInviteRegNumber = 0L;
    private Long androidIndirectInviteRegNumber = 0L;
    private Long iosIndirectInviteRegNumber = 0L;
    private Long validDirectInviteRegNumber = 0L;
    private Long pcValidDirectInviteRegNumber = 0L;
    private Long androidValidDirectInviteRegNumber = 0L;
    private Long iosValidDirectInviteRegNumber = 0L;
    private Long validIndirectInviteRegNumber = 0L;
    private Long pcValidIndirectInviteRegNumber = 0L;
    private Long androidValidIndirectInviteRegNumber = 0L;
    private Long iosValidIndirectInviteRegNumber = 0L;
    private Long lastId;
    private java.sql.Timestamp created;
    private java.sql.Timestamp updated;

    public static RegStatistic defaultAggregateInstance(Long orgId) {
        RegStatistic regStatistic = new RegStatistic();
        regStatistic.setOrgId(orgId);
        regStatistic.setCreated(new Timestamp(System.currentTimeMillis()));
        regStatistic.setUpdated(new Timestamp(System.currentTimeMillis()));
        regStatistic.setLastId(0L);
        regStatistic.setAggregate(1);
        regStatistic.setStatisticDate("total");
        return regStatistic;
    }


    public void incrRegNumber(Integer incrBy) {
        regNumber = regNumber + incrBy;
    }

    public void incrPcRegNumber(Integer incrBy) {
        pcRegNumber = pcRegNumber + incrBy;
    }

    public void incrAndroidRegNumber(Integer incrBy) {
        androidRegNumber = androidRegNumber + incrBy;
    }

    public void incrIosRegNumber(Integer incrBy) {
        iosRegNumber = iosRegNumber + incrBy;
    }

    public void incrNotInvitedRegNumber(Integer incrBy) {
        notInvitedRegNumber = notInvitedRegNumber + incrBy;
    }

    public void incrPcNotInvitedRegNumber(Integer incrBy) {
        pcNotInvitedRegNumber = pcNotInvitedRegNumber + incrBy;
    }

    public void incrAndroidNotInvitedRegNumber(Integer incrBy) {
        androidNotInvitedRegNumber = androidNotInvitedRegNumber + incrBy;
    }

    public void incrIosNotInvitedRegNumber(Integer incrBy) {
        iosNotInvitedRegNumber = iosNotInvitedRegNumber + incrBy;
    }

    public void incrInviteRegNumber(Integer incrBy) {
        inviteRegNumber = inviteRegNumber + incrBy;
    }

    public void incrPcInviteRegNumber(Integer incrBy) {
        pcInviteRegNumber = pcInviteRegNumber + incrBy;
    }

    public void incrAndroidInviteRegNumber(Integer incrBy) {
        androidInviteRegNumber = androidInviteRegNumber + incrBy;
    }

    public void incrIosInviteRegNumber(Integer incrBy) {
        iosInviteRegNumber = iosInviteRegNumber + incrBy;
    }

    public void incrDirectInviteRegNumber(Integer incrBy) {
        directInviteRegNumber = directInviteRegNumber + incrBy;
    }

    public void incrPcDirectInviteRegNumber(Integer incrBy) {
        pcDirectInviteRegNumber = pcDirectInviteRegNumber + incrBy;
    }

    public void incrAndroidDirectInviteRegNumber(Integer incrBy) {
        androidDirectInviteRegNumber = androidDirectInviteRegNumber + incrBy;
    }

    public void incrIosDirectInviteRegNumber(Integer incrBy) {
        iosDirectInviteRegNumber = iosDirectInviteRegNumber + incrBy;
    }

    public void incrIndirectInviteRegNumber(Integer incrBy) {
        indirectInviteRegNumber = indirectInviteRegNumber + incrBy;
    }

    public void incrPcIndirectInviteRegNumber(Integer incrBy) {
        pcIndirectInviteRegNumber = pcIndirectInviteRegNumber + incrBy;
    }

    public void incrAndroidIndirectInviteRegNumber(Integer incrBy) {
        androidIndirectInviteRegNumber = androidIndirectInviteRegNumber + incrBy;
    }

    public void incrIosIndirectInviteRegNumber(Integer incrBy) {
        iosIndirectInviteRegNumber = iosIndirectInviteRegNumber + incrBy;
    }

    public void incrValidDirectInviteRegNumber(Integer incrBy) {
        validDirectInviteRegNumber = validDirectInviteRegNumber + incrBy;
    }

    public void incrPcValidDirectInviteRegNumber(Integer incrBy) {
        pcValidDirectInviteRegNumber = pcValidDirectInviteRegNumber + incrBy;
    }

    public void incrAndroidValidDirectInviteRegNumber(Integer incrBy) {
        androidValidDirectInviteRegNumber = androidValidDirectInviteRegNumber + incrBy;
    }

    public void incrIosValidDirectInviteRegNumber(Integer incrBy) {
        iosValidDirectInviteRegNumber = iosValidDirectInviteRegNumber + incrBy;
    }

    public void incrValidIndirectInviteRegNumber(Integer incrBy) {
        validIndirectInviteRegNumber = validIndirectInviteRegNumber + incrBy;
    }

    public void incrPcValidIndirectInviteRegNumber(Integer incrBy) {
        pcValidIndirectInviteRegNumber = pcValidIndirectInviteRegNumber + incrBy;
    }

    public void incrAndroidValidIndirectInviteRegNumber(Integer incrBy) {
        androidValidIndirectInviteRegNumber = androidValidIndirectInviteRegNumber + incrBy;
    }

    public void incrIosValidIndirectInviteRegNumber(Integer incrBy) {
        iosValidIndirectInviteRegNumber = iosValidIndirectInviteRegNumber + incrBy;
    }

//    public static void main(String[] args) {
////        Field[] fields = RegStatistic.class.getDeclaredFields();
////        for (Field field : fields){
////            String name = field.getName();
////            String content = "public void incr" + name.substring(0,1).toUpperCase() + name.substring(1) +"(Integer incrBy){\n  "+name+" = "+name+" + incrBy;\n}";
////
////        }
//
////        Field[] fields = RegStatistic.class.getDeclaredFields();
////        for (Field field : fields){
////            String name = field.getName();
////            String upperName =  name.substring(0,1).toUpperCase() + name.substring(1);
////            StringBuffer content = new StringBuffer();
////            content.append("s.set").append(upperName).append("(s1.get").append(upperName).append("() + s2.get").append(upperName).append("());");
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
//        Field[] fields = RegStatistic.class.getDeclaredFields();
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
