package com.youzan.filebackup.files;

/**
 * Created by lin on 17/4/10.
 */
public class BackupLocation {
    private long backupFileIndex;
    private long backupFileOffset;

    public BackupLocation(long backupFileIndex, long backupFileOffset) {
        this.backupFileIndex = backupFileIndex;
        this.backupFileOffset = backupFileOffset;
    }

    public long getBackupFileIndex() {
        return this.backupFileIndex;
    }

    public long getBackupFileOffset() {
        return this.backupFileOffset;
    }

    public static boolean hasOffset(BackupLocation a, BackupLocation b){
        if(b.backupFileIndex < a.backupFileIndex)
            return false;
        boolean diffFile = b.backupFileIndex > a.backupFileIndex;
        if(!diffFile)
            return b.backupFileOffset > a.backupFileOffset;
        return diffFile;
    }

    public String toString() {
        return "BackupFileIndex: " + this.backupFileIndex +", BackupFileOffset: " + this.backupFileOffset;
    }
}
