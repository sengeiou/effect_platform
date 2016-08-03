package com.ali.lz.effect.tools.config2xml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ali.lz.effect.tools.util.Constants;
import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * 效果平台数据库访问类
 * 
 * @author jiuling.ypf
 */
public class MySQLManager {

    private static final Log LOG = LogFactory.getLog(MySQLManager.class);

    private JDBCUtil jdbcUtil = null;

    /**
     * 构造函数
     */
    public MySQLManager(JDBCUtil jdbcUtil) {
        this.jdbcUtil = jdbcUtil;
    }

    /**
     * 查询计划共享表plan_share中所有记录
     * 
     * @return
     */
    public List<PlanShareResult> queryPlanShare() {
        List<PlanShareResult> list = new ArrayList<PlanShareResult>();
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn.prepareStatement("SELECT plan_id, type FROM plan_share");
            rs = pstmQuery.executeQuery();
            while (rs.next()) {
                list.add(new PlanShareResult(rs.getInt(1), rs.getInt(2)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closeResultSet(rs);
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return list;
    }

    /**
     * 根据计划共享表plan_share中记录，查询计划配置表plan_config中相关信息
     * 
     * @return
     */
    public List<PlanConfigResult> queryPlanConfig(final List<PlanShareResult> list) {
        List<PlanConfigResult> configList = new ArrayList<PlanConfigResult>();
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn
                    .prepareStatement("SELECT plan_id, user_id, effect_url, src_type, path_type, belong_id, ind_ids, period, expire_type, link_type FROM plan_config WHERE plan_id = ?");
            for (int i = 0; i < list.size(); i++) {
                PlanShareResult planShareRecord = (PlanShareResult) list.get(i);
                if (planShareRecord.getType() == Constants.PLAN_SHARE_TYPE_COMPLETE) {
                    pstmQuery.setInt(1, planShareRecord.getPlanId());
                    rs = pstmQuery.executeQuery();
                    if (rs.next()) {
                        configList.add(new PlanConfigResult(rs.getInt(1), rs.getInt(2), rs.getString(3), rs
                                .getString(4), rs.getInt(5), rs.getInt(6), rs.getString(7), rs.getInt(8), rs.getInt(9),
                                rs.getString(10)));
                    } else {
                        LOG.error("plan_id: " + planShareRecord.getPlanId() + " not exists in plan_config table");
                    }
                    jdbcUtil.closeResultSet(rs);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return configList;
    }

    /**
     * 根据src_type，查询自定义来源表src_custom_config中相关记录的src_url
     * 
     * @return
     */
    public List<String> querySrcCustomConfig(final String[] srcTypes) {
        List<String> srcUrlList = new ArrayList<String>();
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn.prepareStatement("SELECT src_url FROM src_custom_config WHERE src_id = ?");
            for (int i = 0; i < srcTypes.length; i++) {
                Integer srcId = Integer.parseInt(srcTypes[i]);
                pstmQuery.setInt(1, srcId.intValue());
                rs = pstmQuery.executeQuery();
                if (rs.next()) {
                    srcUrlList.add(rs.getString(1));
                } else {
                    LOG.error("src_id: " + srcId + " not exists in src_custom_config table");
                }
                jdbcUtil.closeResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return srcUrlList;
    }

    /**
     * 根据plan_id，查询路径表path_config中相关的path_id
     * 
     * @return
     */
    public List<Integer> queryPathId(final int planId) {
        List<Integer> pathIdList = new ArrayList<Integer>();
        int pathId = 0;
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn.prepareStatement("SELECT path_id FROM path_config WHERE plan_id = ? AND deleted = 0");
            pstmQuery.setInt(1, planId);
            rs = pstmQuery.executeQuery();
            while (rs.next()) {
                pathId = rs.getInt(1);
                pathIdList.add(pathId);
            }
            jdbcUtil.closeResultSet(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return pathIdList;
    }

    /**
     * 根据path_id，查询路径表path_config中相关记录的data
     * 
     * @return
     */
    public String queryPathConfig(final int pathId) {
        String pathData = null;
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn.prepareStatement("SELECT data FROM path_config WHERE path_id = ? AND deleted = 0");
            pstmQuery.setInt(1, pathId);
            rs = pstmQuery.executeQuery();
            if (rs.next()) {
                pathData = rs.getString(1);
            } else {
                LOG.error("path_id: " + pathId + " not exists in path_config table");
            }
            jdbcUtil.closeResultSet(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return pathData;
    }

    /**
     * 根据path_id，查询路径表path_config中相关记录的data
     * 
     * @return
     */
    public List<String> queryPathConfig(final String[] pathIds) {
        List<String> dataList = new ArrayList<String>();
        Connection conn = jdbcUtil.getConnection(false);
        PreparedStatement pstmQuery = null;
        ResultSet rs = null;
        try {
            pstmQuery = conn.prepareStatement("SELECT data FROM path_config WHERE path_id = ? AND deleted = 0");
            for (int i = 0; i < pathIds.length; i++) {
                Integer pathId = Integer.parseInt(pathIds[i]);
                pstmQuery.setInt(1, pathId.intValue());
                rs = pstmQuery.executeQuery();
                if (rs.next()) {
                    dataList.add(rs.getString(1));
                } else {
                    LOG.error("path_id: " + pathId + " not exists in path_config table");
                }
                jdbcUtil.closeResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
        } finally {
            jdbcUtil.closePreparedStatement(pstmQuery);
            jdbcUtil.closeConnection(conn);
        }
        return dataList;
    }

    /**
     * 删除计划共享表plan_share中已同步过的记录 注意：这里没有直接删除表中所有记录，是为了防止出现数据同步过程中，正好有新记录插入的情况！
     * 
     * @param list
     * @return
     */
    public boolean deletePlanShare(final List<PlanShareResult> list) {
        Connection conn = jdbcUtil.getConnection(true);
        PreparedStatement pstmDelete = null;
        try {
            pstmDelete = conn.prepareStatement("DELETE FROM plan_share WHERE plan_id = ? AND type = ?");
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                PlanShareResult planShareRecord = (PlanShareResult) list.get(i);
                pstmDelete.setInt(1, planShareRecord.getPlanId());
                pstmDelete.setInt(2, planShareRecord.getType());
                pstmDelete.addBatch();
            }
            pstmDelete.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e);
            try { // 批量更新失败，则进行回滚操作
                if (!conn.isClosed()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
                LOG.error(e1);
            }
            return false;
        } finally {
            jdbcUtil.closePreparedStatement(pstmDelete);
            jdbcUtil.closeConnection(conn);
        }
    }

}
