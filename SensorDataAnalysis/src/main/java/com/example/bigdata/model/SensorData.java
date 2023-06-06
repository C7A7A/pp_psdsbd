package com.example.bigdata.model;

public class SensorData {
    private Integer value;
    private String sensor;
    private Long timestamp;

    // Constructors
    public SensorData() {
        this(-1, "", -1L);
    }

    public SensorData(Integer value, String sensor, Long timestamp) {
        this.value = value;
        this.sensor = sensor;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "value=" + value +
                ", sensor='" + sensor + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
