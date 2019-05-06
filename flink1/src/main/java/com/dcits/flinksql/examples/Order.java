package com.dcits.flinksql.examples;

public class Order {
	public Long users;
	public String product;
	public int amount;

	public Order(Long user, String product, int amount) {
		this.users = user;
		this.product = product;
		this.amount = amount;
	}
	public Order(){}
	public Order(String x) {
		this.users = Long.parseLong(x.split(",")[0]);
		this.product = x.split(",")[1];
		this.amount = Integer.parseInt(x.split(",")[0]);
	}
	
	@Override
	public String toString() {
		return "Order{" +
			"user=" + users +
			", product='" + product + '\'' +
			", amount=" + amount +
			'}';
	}
}
