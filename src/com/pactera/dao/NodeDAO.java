package com.pactera.dao;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import com.pactera.entity.DataNode;
import com.pactera.entity.HistoryNode;
import com.pactera.util.DatabaseUtil;

public class NodeDAO {

	public DataNode getLatestVoltage(String number) {
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "select * from t_pi where c_name='Node_1_"+number+"' order by d_dateline desc";
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
				dn.setTimestamp(rs.getDate("d_dateline"));
			}

		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			DatabaseUtil.beFree(rs, st, conn);
		}

		return dn;
	}
	
	public List<HistoryNode> getVoltageHistory(String number){
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "select d_dateline,c_value from t_pi where c_name='Node_1_"+number+"'";
		List<HistoryNode> list =new ArrayList<HistoryNode>();
		HistoryNode hd=null;
		
		try {
			conn = DatabaseUtil.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			
			while(rs.next()){
				hd=new HistoryNode();
				hd.setValue(rs.getString("c_value"));
//				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				hd.setTime(formatter.parse(rs.getTimestamp("d_dateline").toString()).getTime());
				hd.setTime(rs.getTimestamp("d_dateline").getTime());
				list.add(hd);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DatabaseUtil.beFree(rs, st, conn);
		}

		return list;
	}
}
