/**
 * 
 */
package com.ali.lz.effect.tools.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.ali.lz.effect.tools.util.Constants;

/**
 * 月光宝盒HBase表的管理类
 * 
 * @author jiuling.ypf
 * 
 */
public class TableUtil {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        if (args.length < 2) {
            System.out.println("Invalid number of parameters: >= 2 parameters!");
            System.out.println("Usage: ");
            System.out
                    .println("       com.etao.lz.effect.tools.table.TableUtil [operation] [tablename1] [tablename2] [...]");
            System.out.println("       [operation]: createTable,dropTable,deleteTable");
            System.out.println("       [tablename]: effect_rpt,effect_rpt_sum,effect_rpt_adclk,effect_rpt_sum_bysrc,");
            System.out
                    .println("                    effect_rt_rpt,effect_rt_rpt_sum,effect_rt_rpt_adclk,effect_rt_rpt_sum_bysrc,effect_rt_traffic_index");
            System.out.println("Example: ");
            System.out
                    .println("       com.etao.lz.effect.tools.table.TableUtil createTable effect_rpt effect_rpt_sum effect_rpt_adclk effect_rpt_sum_bysrc");
            System.out
                    .println("       com.etao.lz.effect.tools.table.TableUtil createTable effect_rt_rpt effect_rt_rpt_sum effect_rt_rpt_adclk effect_rt_rpt_sum_bysrc effect_rt_traffic_index");
            System.exit(1);
        }

        // 支持的HBase表管理类型
        final String[] operationArray = { "createTable", "dropTable", "deleteTable" };

        // 支持的Hbase表名称列表
        final List<String> tableList = new ArrayList<String>();
        tableList.addAll(Arrays.asList(Constants.OFFLINE_HBASE_TABLE_NAMES));
        tableList.addAll(Arrays.asList(Constants.ONLINE_HBASE_TABLE_NAMES));
        tableList.addAll(Arrays.asList(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES));

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }

        String operation = args[0];
        if (operation.equalsIgnoreCase(operationArray[0])) { // 创建表操作
            CreateTable createTable = new CreateTable(admin);
            for (int i = 1; i < args.length; i++) {
                if (!tableList.contains(args[i])) {
                    System.err.println("Invalid tablename: " + args[i] + ", skip it");
                    continue;
                }
                createTable.doWork(args[i]);
            }
        } else if (operation.equalsIgnoreCase(operationArray[1])) { // 删除表操作
            DropTable dropTable = new DropTable(admin);
            for (int i = 1; i < args.length; i++) {
                if (!tableList.contains(args[i])) {
                    System.err.println("Invalid tablename: " + args[i] + ", skip it");
                    continue;
                }
                dropTable.doWork(args[i]);
            }
        } else if (operation.equalsIgnoreCase(operationArray[2])) { // 删除表中数据操作
            DropTable dropTable = new DropTable(admin);
            CreateTable createTable = new CreateTable(admin);
            for (int i = 1; i < args.length; i++) {
                if (!tableList.contains(args[i])) {
                    System.err.println("Invalid tablename: " + args[i] + ", skip it");
                    continue;
                }
                dropTable.doWork(args[i]);
                createTable.doWork(args[i]);
            }
        } else { // 非法操作
            System.err.println("Invalid operation: " + operation + ", terminate it");
        }
    }

}
