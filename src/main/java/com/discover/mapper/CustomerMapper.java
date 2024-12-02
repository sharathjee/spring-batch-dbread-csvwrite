package com.discover.mapper;

import com.discover.model.Customer;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class CustomerMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
        Customer customer = new Customer();
        customer.setId(rs.getLong("id"));
        customer.setFirst_name(rs.getString("first_name"));
        customer.setLast_name(rs.getString("last_name"));
        customer.setEmail(rs.getString("email"));
        customer.setGender(rs.getString("gender"));
        customer.setPhone(rs.getString("phone"));
        customer.setOrders(rs.getInt("orders"));
        customer.setHobbies(rs.getString("hobbies"));
        customer.setAge(rs.getInt("age"));
        customer.setSpent(rs.getFloat("spent"));
        customer.setIs_married(rs.getBoolean("is_married"));
        customer.setRegistered(rs.getDate("registered"));
        customer.setJob(rs.getString("job"));
        return customer;
    }
}
