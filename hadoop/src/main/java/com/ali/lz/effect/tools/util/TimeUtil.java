package com.ali.lz.effect.tools.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间相关工具类
 * 
 * @author jiuling.ypf
 * 
 */
public class TimeUtil {

    /**
     * 获得当天开始的时间：年月日的int值，如20120615
     * 
     * @return
     */
    public static int getToday() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss");
        Calendar curTime = Calendar.getInstance(); // today time
        String todayDate = formatter.format(curTime.getTime()); // today string
        String dayDate = todayDate.substring(0, 8); // day date

        return Integer.parseInt(dayDate);
    }

    /**
     * 获得昨天开始的时间：年月日的int值，如20120614
     * 
     * @return
     */
    public static int getLastday() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss");

        // calc the next running time 00:00:00
        Calendar curTime = Calendar.getInstance(); // today time

        String todayDate = formatter.format(curTime.getTime()); // today string
        String timeDate = todayDate.substring(0, 8); // today date
        timeDate = timeDate + " 00-00-00"; // today 00:00:00

        Date dateBegin = null;
        try {
            dateBegin = formatter.parse(timeDate);
            long lastDayTimeStamp = dateBegin.getTime() - 1000 * 3600 * 24;

            Date lastDayDate = new Date(lastDayTimeStamp);

            String lastDayDateStr = formatter.format(lastDayDate); // last day
            // string
            String lastDayStr = lastDayDateStr.substring(0, 8); // last day date

            return Integer.parseInt(lastDayStr);
        } catch (ParseException e1) {
            e1.printStackTrace();
            return 0;
        }
    }

    /**
     * 获得指定日期的后一天：年月日的int值
     * 
     * @param dataDate
     * @return
     */
    public static int getNextday(int dataDate) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss");
        String timeDate = dataDate + " 00-00-00";

        Date dateBegin = null;
        try {
            dateBegin = formatter.parse(timeDate);
            long nextDayTimeStamp = dateBegin.getTime() + 1000 * 3600 * 24;
            Date nextDayDate = new Date(nextDayTimeStamp);
            String nextDayDateStr = formatter.format(nextDayDate); // next day
            // string
            String nextDayStr = nextDayDateStr.substring(0, 8); // next day date
            return Integer.parseInt(nextDayStr);
        } catch (ParseException e1) {
            e1.printStackTrace();
            return 0;
        }
    }

    /**
     * 从yyyyMMdd转换成Unix时间戳
     * 
     * @param dataDate
     * @return
     */
    public static int getDayTs(int dataDate) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss");
        String timeDate = dataDate + " 00-00-00";

        Date dateBegin = null;
        try {
            dateBegin = formatter.parse(timeDate);
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
        long dayTimeStamp = dateBegin.getTime();
        return (int) (dayTimeStamp / 1000);
    }

    /**
     * 从yyyyMMdd转换成Unix时间戳
     * 
     * @param dataDate
     * @return
     */
    public static int getNextdayTs(int dataDate) {
        int dayTs = getDayTs(dataDate);
        return (dayTs + 3600 * 24);
    }

    /**
     * 获取今天的年月日 格式：yyyy/MM/dd
     * 
     * @return
     */
    public static String getTodayDir() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH-mm-ss");
        Calendar curTime = Calendar.getInstance();
        String todayDate = formatter.format(curTime.getTime());
        return todayDate.substring(0, 10);
    }

}
