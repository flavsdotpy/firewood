package com.ap3x.firewood.models;

public class DBOffset {

    private Long lastExecution;
    private String sourceKey;
    private String message;

    public DBOffset() {
    }

    public DBOffset(final Long lastExecution, final String sourceKey, final String message) {
        this.lastExecution = lastExecution;
        this.sourceKey = sourceKey;
        this.message = message;
    }

    public Long getLastExecution() {
        return lastExecution;
    }

    public void setLastExecution(Long lastExecution) {
        this.lastExecution = lastExecution;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
