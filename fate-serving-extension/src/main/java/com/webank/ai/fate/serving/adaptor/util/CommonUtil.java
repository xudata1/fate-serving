package com.webank.ai.fate.serving.adaptor.util;

import com.webank.ai.fate.serving.core.bean.BatchHostFederatedParams;
import com.webank.ai.fate.serving.core.bean.BatchInferenceRequest;

import java.sql.*;
import java.util.*;

public class CommonUtil {

    /**
     * 通用的查询
     * @param featureIds 传递的参数信息
     * @return
     */
    public static String viewMockAdapter(Map<String, Object> featureIds) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = JdbcUtil.getConnection();
            String sql = "select * from " + featureIds.get("namespace") + " where id = ?";
            preparedStatement = connection.prepareStatement(sql); // 预编译sql
           // 替换sql中的所有?为实参.
                preparedStatement.setObject( 1, featureIds.get("featureId"));

            resultSet = preparedStatement.executeQuery(); // 执行查询
            ResultSetMetaData metaData = resultSet.getMetaData(); // metaData对象中包含的是虚表的表结构等原始数据
            int columnCount = metaData.getColumnCount(); // 获取虚表的列数

            List<String> columnlist = new ArrayList<String>();
            for (int i = 0; i < columnCount; i++) { // 遍历所有列
                String columnLabel = metaData.getColumnLabel(i + 1); // 根据列索引依次获取列标签
                columnlist.add(columnLabel);//将索引标签添加到list中等待输出
            }

            List<String> taledata = new ArrayList<String>();
            while (resultSet.next()) { // 遍历数据
                String line = "";
                for (int i = 0; i < columnCount; i++) { // 再一次遍历所有列
                    String columnLabel = metaData.getColumnLabel(i + 1); // 动态取出各个列标签
                    Object value = resultSet.getObject(columnLabel); // 根据列标签再取实际值, 更灵活
                   line += columnlist.get(i) +":"+value+",";
                }
                int length = line.length();
                String linesubstring = line.substring(0, length - 1);
                taledata.add(linesubstring);
            }
            if (taledata.size()!= 1){
                throw new SQLException("id对应数据不唯一");
            }
        return taledata.get(0);
        } finally {
            JdbcUtil.close(connection, preparedStatement, resultSet);
        }
    }



    /**
     * 通用的查询
     * @param featureIdList 传递的参数信息
     * @return
     */
    public static HashMap<BatchInferenceRequest.SingleInferenceData, String> viewMockBatchAdapter(List<BatchHostFederatedParams.SingleInferenceData> featureIdList) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = JdbcUtil.getConnection();


            HashMap<BatchInferenceRequest.SingleInferenceData, String> tableDataHashMap = new HashMap<>();//保存输出结果


            for (BatchInferenceRequest.SingleInferenceData singleInferenceData : featureIdList) {
                String sql = "select * from " + singleInferenceData.getSendToRemoteFeatureData().get("namespace") + " where id = ?";
                preparedStatement = connection.prepareStatement(sql); // 预编译sql
                // 替换sql中的所有?为实参.
                preparedStatement.setObject(1, singleInferenceData.getSendToRemoteFeatureData().get("featureId"));

                resultSet = preparedStatement.executeQuery(); // 执行查询
                ResultSetMetaData metaData = resultSet.getMetaData(); // metaData对象中包含的是虚表的表结构等原始数据
                int columnCount = metaData.getColumnCount(); // 获取虚表的列数

                List<String> columnlist = new ArrayList<String>();
                for (int i = 0; i < columnCount; i++) { // 遍历所有列
                    String columnLabel = metaData.getColumnLabel(i + 1); // 根据列索引依次获取列标签
                    columnlist.add(columnLabel);//将索引标签添加到list中等待输出
                }

                List<String> taledata = new ArrayList<String>();//存储结果
                while (resultSet.next()) { // 遍历数据
                    String line = "";
                    for (int i = 0; i < columnCount; i++) { // 再一次遍历所有列
                        String columnLabel = metaData.getColumnLabel(i + 1); // 动态取出各个列标签
                        Object value = resultSet.getObject(columnLabel); // 根据列标签再取实际值, 更灵活
                        line += columnlist.get(i) + ":" + value + ",";
                    }

                    int length = line.length();
                    String linesubstring = line.substring(0, length - 1);
                    taledata.add(linesubstring);
                }

                tableDataHashMap.put(singleInferenceData, taledata.get(0));
                if (taledata.size()!= 1){
                    throw new SQLException("id对应数据不唯一");
                }
            }
            return tableDataHashMap;

        } finally {
            JdbcUtil.close(connection, preparedStatement, resultSet);
        }
    }


}
