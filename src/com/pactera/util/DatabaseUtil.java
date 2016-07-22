package com.pactera.util;

import java.sql.*;

public class DatabaseUtil {

	private DatabaseUtil() {
	}

	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static Connection getConnection() throws SQLException {
//		String userName = "postgres";
//		String userPwd = "SuperROOT2016";
//		String dbURL = "jdbc:postgresql://localhost:5432/lili";
//		String userName = "u9e6c906fb58849a49beac997d16eca92";
//		String userPwd = "133ba0cc8cc24597994ad3784c00c8de";
//		String dbURL = "jdbc:postgresql://10.72.6.143:5432/d9c2333a4cb874b87b041ade2d25a3f93";
		String userName = "u328edf88d4fc43689ef2d170e84d887c";
		String userPwd = "93c309d1c8454c35afbea0c2bb68fa50";
		String dbURL = "jdbc:postgresql://10.72.6.134:5432/d328edf88d4fc43689ef2d170e84d887c";
		String driverName = "org.postgresql.Driver";

		Connection dbConn = null;
		try {
			Class.forName(driverName);
			dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
			System.out.println("Connection Successful!"); // 如果连接成功
															// 控制台输出Connection
															// Successful!
		} catch (Exception e) {
			e.printStackTrace();
		}

		return dbConn;
	}

	public static void free(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public static void beFree(ResultSet rs, Statement st, Connection conn) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (st != null) {
					st.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
