package ca.opax.kafka.model;

public class DataRecord {

    Long count;
    String message;

    public DataRecord() {
    }

    public DataRecord(Long count, String message) {
        this.count = count;
        this.message = message;
    }

    public Long getCount() {
        return count;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

}
