package com.ap3x.firewood.models;

public class FileMetadata {

    private String filePath;
    private Long fileExportTimestamp;
    private String outputRef;
    private Long executionTime;
    private Boolean result;
    private Long totalLines;
    private Long successLines;
    private String exceptionMessage;

    public FileMetadata() {
    }

    public FileMetadata(final String filePath,
                        final Long fileExportTimestamp){
        this.filePath = filePath;
        this.fileExportTimestamp = fileExportTimestamp;
        this.result = null;
        this.totalLines = null;
        this.exceptionMessage = null;
        this.executionTime = null;
        this.successLines = null;
        this.outputRef = null;
    }

    public FileMetadata(final Long executionTime,
                        final Boolean result,
                        final String filePath,
                        final Long fileExportTimestamp,
                        final Long totalLines,
                        final Long successLines,
                        final String exceptionMessage,
                        final String outputRef) {
        this.result = result;
        this.filePath = filePath;
        this.totalLines = totalLines;
        this.exceptionMessage = exceptionMessage;
        this.executionTime = executionTime;
        this.successLines = successLines;
        this.outputRef = outputRef;
        this.fileExportTimestamp = fileExportTimestamp;
    }

    public Long getSuccessLines() {
        return successLines;
    }

    public void setSuccessLines(Long successLines) {
        this.successLines = successLines;
    }

    public Long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Long executionTime) {
        this.executionTime = executionTime;
    }

    public Boolean getResult() {
        return result;
    }

    public void setResult(Boolean result) {
        this.result = result;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Long getTotalLines() {
        return totalLines;
    }

    public void setTotalLines(Long totalLines) {
        this.totalLines = totalLines;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public String getOutputRef() {
        return outputRef;
    }

    public void setOutputRef(String outputRef) {
        this.outputRef = outputRef;
    }

    public Long getFileExportTimestamp() {
        return fileExportTimestamp;
    }

    public void setFileExportTimestamp(Long fileExportTimestamp) {
        this.fileExportTimestamp = fileExportTimestamp;
    }
}
