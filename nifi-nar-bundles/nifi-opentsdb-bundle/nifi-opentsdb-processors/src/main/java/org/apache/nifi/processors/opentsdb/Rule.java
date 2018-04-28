package org.apache.nifi.processors.opentsdb;

/**
 * Description:Converting rule
 *
 * @author bright
 */
public class Rule {
    /**
     * object field name
     */
    private String field;

    /**
     * the metric name of the OpenTSDB Data Point
     */
    private String metric;

    public Rule() {
    }

    public Rule(String field, String metric) {
        this.field = field;
        this.metric = metric;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }
}
