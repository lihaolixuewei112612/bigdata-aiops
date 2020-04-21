package com.dtc.java.analytic.V1.test1.broadcast;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

	/**
	 * Metric name
	 */
	private String name;

	/**
	 * Metric timestamp
	 */
	private Long timestamp;

	/**
	 * Metric fields
	 */
//	private Map<String, Object> fields;

	/**
	 * Metric tags
	 */
	private Map<String, String> tags;

	public void setName(String name) {
		this.name = name;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

//	public void setFields(Map<String, Object> fields) {
//		this.fields = fields;
//	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
}
