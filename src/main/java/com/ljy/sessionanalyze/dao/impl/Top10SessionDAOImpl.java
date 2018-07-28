package com.ljy.sessionanalyze.dao.impl;

import com.ljy.sessionanalyze.dao.ITop10SessionDAO;
import com.ljy.sessionanalyze.domain.Top10Session;
import com.ljy.sessionanalyze.jdbc.JDBCHelper;

/**
 * top10活跃session的DAO实现
 * @author Administrator
 *
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

	@Override
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_category_session values(?,?,?,?)";
		
		Object[] params = new Object[]{top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
