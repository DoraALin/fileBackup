package com.youzan.filebackup.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lin on 17/4/10.
 */
public class BackupScopeConfig {
    private final static Logger logger = LoggerFactory.getLogger(BackupScopeConfig.class);
    //5MB per backup file
    private volatile long backupFileMaxByte = 5 * 1024 * 1024;
    //100KB default value
    private volatile long backupContentBufferSizeInByte = 100 * 1024;
    private int writeExecutorTerminationAwaitTimeoutInSecond = 10;

    public static final int BACKUP_ITEM_MAX_SIZE_IN_BYTE = 4;
    public static final int BACKUP_FILE_MAX_SIZE_IN_BYTE = 8;

    public long getBackupFileMaxByte() {
        return backupFileMaxByte;
    }

    public int getWriteExecutorTerminationAwaitTimeoutInSecond() {
        return this.writeExecutorTerminationAwaitTimeoutInSecond;
    }

    public BackupScopeConfig setWriteExecutorTerminationAwaitTimeoutInSecond(int terminationTimeout) {
        if(terminationTimeout < 0)
            throw new IllegalArgumentException("Termination timeout could not be negative");
        this.writeExecutorTerminationAwaitTimeoutInSecond = terminationTimeout;
        return this;
    }

    public BackupScopeConfig setBackupFileMaxByte(long newBackupFileMaxByte) {
        if(newBackupFileMaxByte <= 0)
            throw new IllegalArgumentException("Negative value is not accepted.");
        this.backupFileMaxByte = newBackupFileMaxByte;
        return this;
    }

    public long getBackupContentBufferSizeInByte() {
        return backupContentBufferSizeInByte;
    }

    public BackupScopeConfig setBackupContentBufferSizeInByte(long newBufferSizeInByte) {
        if(newBufferSizeInByte <= 0)
            throw new IllegalArgumentException("Negative value is not accepted.");
        this.backupContentBufferSizeInByte = newBufferSizeInByte;
        return this;
    }
}
