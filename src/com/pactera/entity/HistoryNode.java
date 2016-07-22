package com.pactera.entity;

public class HistoryNode {

	private long time;
	private String value;
//	private String num;

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

//	public String getNum() {
//		return num;
//	}
//
//	public void setNum(String num) {
//		this.num = num;
//	}

	@Override
	public String toString() {
		return "HistoryNode [time=" + time + ", value=" + value + "]";
	}
}
