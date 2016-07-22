package com.pactera.entity;

import java.util.Date;

public class DataNode {

	private String address;
	private String quality;
	private String name;
	private String category;
	private String value;
	private Date timestamp;
	private int id;


	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getQuality() {
		return quality;
	}

	public void setQuality(String quality) {
		this.quality = quality;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "DataNode [address=" + address + ", quality=" + quality + ", name=" + name + ", category=" + category
				+ ", value=" + value + ", timestamp=" + timestamp + ", id=" + id + "]";
	}
}
