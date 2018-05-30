package com.sapient;

import java.io.Serializable;

public class Customer implements Serializable {

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
		 * 
		 */
	private static final long serialVersionUID = -3666165574540337970L;
	public int id;
	public String name;

	public Customer() {
		// TODO Auto-generated constructor stub
	}

	public Customer(String string, int i) {
		this.name = name;
		this.id = id;
	}

	@Override
	public String toString() {
		return "Customer [id=" + id + ", name=" + name + "]";
	}
	

}
