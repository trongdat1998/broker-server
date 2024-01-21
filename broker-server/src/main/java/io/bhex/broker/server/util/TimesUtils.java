package io.bhex.broker.server.util;

import java.util.Calendar;
import java.util.Date;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-05-22 13:32
 */
public class TimesUtils {

    /**
     * 获取当日开始时间
     * @param timestamp
     * @return
     */
    public static Long getDailyStartTime(Long timestamp){
        Calendar calendar=Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        Date date=calendar.getTime();
        return date.getTime()/1000;
    }
}
