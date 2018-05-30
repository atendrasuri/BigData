package com.sapient;

import java.io.Serializable;

public class Stock implements Serializable {

	private String name;
	private double stock;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getStock() {
		return stock;
	}
	public void setStock(double stock) {
		this.stock = stock;
	}
	
	@Override
	public String toString() {
		return "Stock [name=" + name + ", stock=" + stock + "]";
	}

}
