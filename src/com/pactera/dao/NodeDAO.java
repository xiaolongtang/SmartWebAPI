package com.pactera.dao;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import com.pactera.entity.DataNode;
import com.pactera.entity.HistoryNode;
import com.pactera.entity.MaxMinNode;
import com.pactera.util.DatabaseUtil;

public class NodeDAO {

	public DataNode getLatestValue(String type) {
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "select * from t_pi where c_name='"+type+"' order by d_dateline desc";
		DataNode dn = new DataNode();

		try {
			conn = DatabaseUtil.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			
			if(rs.next()){
				dn.setId(rs.getInt("n_id"));
				dn.setAddress(rs.getString("c_address"));
				dn.setCategory(rs.getString("c_category"));
				dn.setQuality(rs.getString("c_quality"));
				dn.setName(rs.getString("c_name"));
				dn.setValue(rs.getString("c_value"));
				dn.setTimestamp(rs.getTimestamp("d_dateline"));
				dn.setDeviceId(rs.getString("c_deviceid"));
			}

		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			DatabaseUtil.beFree(rs, st, conn);
		}

		return dn;
	}
	
	public MaxMinNode getMaxMinValue(String maxMinValue,String type){
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd");
		String dateStr=dateFormater.format(new Date());
		String sql = "select "+maxMinValue+"(c_value), c_name from t_pi where c_name='"+type+"' and d_dateline>'"+dateStr+"' group by c_name";
		MaxMinNode mmn=new MaxMinNode();
		
		try {
			conn = DatabaseUtil.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			
			if(rs.next()){
				mmn.setValue(rs.getString(maxMinValue));
				mmn.setName(rs.getString("c_name"));
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			DatabaseUtil.beFree(rs, st, conn);
		}
		
		return mmn;
	}

}
