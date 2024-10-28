package com.example.demo_dsp_returning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import javax.sql.DataSource;

import net.ttddyy.dsproxy.ConnectionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.proxy.ProxyConfig;
import net.ttddyy.dsproxy.proxy.RepeatableReadResultSetProxyLogicFactory;
import net.ttddyy.dsproxy.proxy.ResultSetProxyLogic;
import net.ttddyy.dsproxy.proxy.ResultSetProxyLogicFactory;
import net.ttddyy.dsproxy.proxy.SimpleResultSetProxyLogicFactory;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
public class DemoDspReturningApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoDspReturningApplication.class, args);
	}

	@Bean
	ApplicationRunner runner(DataSource dataSource) {
		DataSource proxy =  ProxyDataSourceBuilder.create(dataSource)
				.proxyResultSet(new DynamicResultSetProxyLogicFactory())
				.logQueryToSysOut()
				.buildProxy();
		return args -> {
			String sql = "INSERT INTO employee (name) VALUES (?) RETURNING id";
			Connection conn = proxy.getConnection();
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "Foo");
			ResultSet rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			System.out.println(">>> Generated id=" + id);
			rs.close();
			ps.close();
			conn.close();
		};
	}

	static class DynamicResultSetProxyLogicFactory implements ResultSetProxyLogicFactory {
		private static final SimpleResultSetProxyLogicFactory SIMPLE = new SimpleResultSetProxyLogicFactory();

		private static final RepeatableReadResultSetProxyLogicFactory REPEATABLE = new RepeatableReadResultSetProxyLogicFactory();

		@Override
		public ResultSetProxyLogic create(ResultSet resultSet, List<QueryInfo> queries, ConnectionInfo connectionInfo, ProxyConfig proxyConfig) {
			boolean returningQuery = queries.stream().anyMatch(queryInfo -> queryInfo.getQuery().toLowerCase().contains(" returning "));
			ResultSetProxyLogicFactory delegate = returningQuery ? REPEATABLE : SIMPLE;
			return delegate.create(resultSet, queries, connectionInfo, proxyConfig);
		}
	}
}
