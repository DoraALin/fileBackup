package com.youzan.filebackup.files;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.youzan.filebackup.context.BackupScope;
import com.youzan.filebackup.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lin on 17/4/10.
 */
public class BackupMeta {
    private final static Logger logger = LoggerFactory.getLogger(BackupMeta.class);
    private final BackupScope scope;
    final private Path metaPath;
    private final ReentrantReadWriteLock metaLock = new ReentrantReadWriteLock();
    private BackupMetaInfo metaInfo = null;
    private AtomicInteger status;
    private volatile boolean inSync = false;
    enum Status {
        READY,
        INVALID,
    }

    static class BackupMetaInfo {
        //read start of
        private BackupLocation readStart;
        private BackupLocation readEnd;
        private BackupLocation writeStart;

        public BackupMetaInfo(final BackupLocation readStart, final BackupLocation readEnd, final BackupLocation writeStart) {
            this.readStart = readStart;
            this.readEnd = readEnd;
            this.writeStart = writeStart;
        }

        BackupLocation getReadStart() {
            return this.readStart;
        }

        BackupLocation getReadEnd() {
            return this.readEnd;
        }

        BackupLocation getWriteStart() {
            return this.writeStart;
        }

        void setReadStart(BackupLocation newLoc) {
            this.readStart = newLoc;
        }

        void setReadEnd(BackupLocation newLoc) {
            this.readEnd = newLoc;
        }

        void setWriteStart(BackupLocation newLoc) {
            this.writeStart = newLoc;
        }
    }

    public boolean isValid() {
        return !(this.status.get() == Status.INVALID.ordinal());
    }

    public BackupMeta(BackupScope scope) {
        this.scope = scope;
        this.metaPath = scope.getMetaPath();
        this.status = new AtomicInteger(Status.READY.ordinal());

        //check if meta file exists
        if(Files.exists(this.metaPath) && !Files.isDirectory(this.metaPath)) {
            try {
                loadMetaFile();
                logger.info("Scope meta-data {} loaded.", this.metaPath);
            } catch (IOException e) {
                logger.error("Scope meta-data file not exist.", scope);
                this.status.set(BackupIndex.Status.INVALID.ordinal());
            }
        }else{
            logger.info("Scope meta-data file not exist. Initialize meta file for backup scope: {}", scope);
            this.metaInfo = new BackupMetaInfo(new BackupLocation(0L, 0L),
                    new BackupLocation(0L, 0L),
                    new BackupLocation(0L, 0L));
            try {
                commitMetaFile();
            } catch (IOException e) {
                logger.error("Scope meta-data file could not commit.", e);
                this.status.set(BackupIndex.Status.INVALID.ordinal());
            }
        }
    }

    //there is no synchronization on loadMetaFile as it is invoked in constructor
    private void loadMetaFile() throws IOException {
        JsonReader jreader = new JsonReader(
                new FileReader(this.metaPath.toAbsolutePath().toString())
        );
        Gson gson = IOUtils.getGson();
        metaInfo = gson.fromJson(jreader, BackupMetaInfo.class);
        jreader.close();
        inSync = true;
    }

    /**
     * Commit to update meta data info in scope meta data.
     * @throws IOException
     */
    public void commitMetaFile() throws IOException {
        if(inSync)
            return;
        try{
            metaLock.writeLock().lock();
            JsonWriter jWriter = new JsonWriter(
                    new FileWriter(this.metaPath.toAbsolutePath().toString())
            );
            Gson gson = IOUtils.getGson();
            gson.toJson(metaInfo, metaInfo.getClass(), jWriter);
            jWriter.close();
            inSync = true;
            logger.info("meta file {} committed.", this.metaPath);
        }finally {
            metaLock.writeLock().unlock();
        }
    }

    public BackupLocation getReadStart() {
        try {
            metaLock.readLock().lock();
            return metaInfo.getReadStart();
        }finally {
            metaLock.readLock().unlock();
        }
    }

    public BackupLocation getReadEnd() {
        try {
            metaLock.readLock().lock();
            return metaInfo.getReadEnd();
        }finally {
            metaLock.readLock().unlock();
        }
    }

    public BackupLocation getWriteStart() {
        try {
            metaLock.readLock().lock();
            return metaInfo.getWriteStart();
        }finally {
            metaLock.readLock().unlock();
        }
    }

    public void setReadStart(BackupLocation newLoc) {
        try{
            metaLock.writeLock().lock();
            metaInfo.setReadStart(newLoc);
            this.inSync = false;
        }finally {
            metaLock.writeLock().unlock();
        }
    }

    public void setReadEnd(BackupLocation newLoc) {
        try{
            metaLock.writeLock().lock();
            metaInfo.setReadEnd(newLoc);
            this.inSync = false;
        }finally {
            metaLock.writeLock().unlock();
        }
    }

    public void setWriteStart(BackupLocation newLoc) {
        try{
            metaLock.writeLock().lock();
            metaInfo.setWriteStart(newLoc);
            this.inSync = false;
        }finally {
            metaLock.writeLock().unlock();
        }
    }
}
