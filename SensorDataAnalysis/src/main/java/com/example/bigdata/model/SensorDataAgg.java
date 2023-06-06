package com.example.bigdata.model;

public class SensorDataAgg {
    private  String sensor;
    private Integer maxVal;
    private Long maxValTimestamp;
    private Integer minVal;
    private Long minValTimestamp;
    private Integer countVal;
    private Integer sumVal;

    // Constructors
    public SensorDataAgg() {
        this("", -1, -1L, -1, -1L, 0, 0);
    }

    public SensorDataAgg(String sensor,
                        Integer maxVal,
                        Long maxValTimestamp,
                        Integer minVal,
                        Long minValTimestamp,
                        Integer countVal,
                        Integer sumVal) {
        this.sensor = sensor;
        this.maxVal = maxVal;
        this.maxValTimestamp = maxValTimestamp;
        this.minVal = minVal;
        this.minValTimestamp = minValTimestamp;
        this.countVal = countVal;
        this.sumVal = sumVal;
    }

    // Getters and Setters
    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public Integer getMaxVal() {
        return maxVal;
    }

    public void setMaxVal(Integer maxVal) {
        this.maxVal = maxVal;
    }

    public Long getMaxValTimestamp() {
        return maxValTimestamp;
    }

    public void setMaxValTimestamp(Long maxValTimestamp) {
        this.maxValTimestamp = maxValTimestamp;
    }

    public Integer getMinVal() {
        return minVal;
    }

    public void setMinVal(Integer minVal) {
        this.minVal = minVal;
    }

    public Long getMinValTimestamp() {
        return minValTimestamp;
    }

    public void setMinValTimestamp(Long minValTimestamp) {
        this.minValTimestamp = minValTimestamp;
    }

    public Integer getCountVal() {
        return countVal;
    }

    public void setCountVal(Integer countVal) {
        this.countVal = countVal;
    }

    public Integer getSumVal() {
        return sumVal;
    }

    public void setSumVal(Integer sumVal) {
        this.sumVal = sumVal;
    }

    @Override
    public String toString() {
        return "SensorDataAgg{" +
                "sensor='" + sensor + '\'' +
                ", maxVal=" + maxVal +
                ", maxValTimestamp=" + maxValTimestamp +
                ", minVal=" + minVal +
                ", minValTimestamp=" + minValTimestamp +
                ", countVal=" + countVal +
                ", sumVal=" + sumVal +
                '}';
    }

}
