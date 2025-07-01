package com.flink.demo.pojo;

import java.util.Objects;

public class WaterSensorVcCount {
    public Integer vc;
    public Long count;

    public Long windowEnd;

    public WaterSensorVcCount(Integer vc, Long count) {
        this.vc = vc;
        this.count = count;
        this.windowEnd = null;
    }

    public WaterSensorVcCount(Integer vc, Long count, Long windowEnd) {
        this.vc = vc;
        this.count = count;
        this.windowEnd = windowEnd;
    }

    public WaterSensorVcCount() {
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WaterSensorVcCount{" +
                "vc=" + vc +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensorVcCount that = (WaterSensorVcCount) o;
        return Objects.equals(vc, that.vc) && Objects.equals(count, that.count) && Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vc, count, windowEnd);
    }
}
