package org.apache.kafka.streams.examples.temperature;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@SuppressWarnings("unchecked")
public class LogData {

    public String timestamp;
    public String nodeName;
    public String k8sName;
    public String offset;
    public String source;
    public String networkId;

    private static final Logger LOG = LoggerFactory.getLogger(LogData.class);

    /**
     * DO NOT REMOVE NEEDED FOR Jackson to deserialize
     */
    public LogData() {
    }

    public LogData(Map<String, Object> jsonMap) {
        try {
            timestamp = getField(jsonMap, "@timestamp");
            nodeName = getField(jsonMap, "nodeName");
            k8sName = getField(jsonMap, "k8sName");
            offset = getField(jsonMap, "offset");
            source = getField(jsonMap, "source");
            networkId = getField(jsonMap, "networkId");
        } catch (Exception e) {
            LOG.error("error converting to LogData {}", jsonMap, e);
            throw e;
        }
    }

    public LogData(final String timestamp,
                   final String nodeName,
                   final String k8sName,
                   final String offset,
                   final String source,
                   final String networkId) {
        this.timestamp = timestamp;
        this.nodeName = nodeName;
        this.k8sName = k8sName;
        this.offset = offset;
        this.source = source;
        this.networkId = networkId;
    }

    private String getField(Map<String, Object> jsonMap, String fieldName) {
        Object value = jsonMap.get(fieldName);
        return value != null ? value.toString() : "dummy_" + fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogData logData = (LogData) o;
        return Objects.equals(timestamp, logData.timestamp) &&
            Objects.equals(nodeName, logData.nodeName) &&
            Objects.equals(k8sName, logData.k8sName) &&
            Objects.equals(offset, logData.offset) &&
            Objects.equals(source, logData.source) &&
            Objects.equals(networkId, logData.networkId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, nodeName, k8sName, offset, source, networkId);
    }

    @Override
    public String toString() {
        return "LogData{" +
            "timestamp='" + timestamp + '\'' +
            ", nodeName='" + nodeName + '\'' +
            ", k8sName='" + k8sName + '\'' +
            ", offset='" + offset + '\'' +
            ", source='" + source + '\'' +
            ", networkId='" + networkId + '\'' +
            '}';
    }
}







