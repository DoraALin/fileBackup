package com.youzan.filebackup.files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.youzan.filebackup.context.BackupScope;
import com.youzan.filebackup.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lin on 17/4/7.
 */
public class BackupIndex {
    private final static Logger logger = LoggerFactory.getLogger(BackupIndex.class);

    final private BackupScope scope;
    final private Path indexPath;
    private final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
    private Map<String, IndexItem> index;
    private AtomicInteger status;
    enum Status {
        READY,
        INVALID,
    }

    static class IndexItem {
        //offset in target backup file
        private long offset;
        //backup file index in current scope
        private long backupFileIndex;
        //length of content associated with index key
        private long length;

        public IndexItem(long backupFileIndex, long offset, long length) {
            this.backupFileIndex = backupFileIndex;
            this.offset = offset;
            this.length = length;
        }

        public long getLength() {
            return this.length;
        }

        public long getBackupFileIndex() {
            return this.backupFileIndex;
        }

        public long getOffset() {
            return this.offset;
        }
    }

    public BackupIndex(BackupScope scope) {
        this.scope = scope;
        this.indexPath = this.scope.getIndexPath();
        index = new ConcurrentHashMap<>();
        this.status = new AtomicInteger(Status.READY.ordinal());
        if(Files.exists(this.indexPath) && !Files.isDirectory(this.indexPath)) {
            try {
                loadIndexFile();
                logger.info("Scope index {} loaded.", this.indexPath);
            } catch (FileNotFoundException e) {
                logger.error("Scope index file not exist.");
                this.status.set(Status.INVALID.ordinal());
            }
        }else{
            logger.info("Scope index does not exist.");
        }
    }

    public IndexItem lookup(String key) {
        try{
            indexLock.readLock().lock();
            return this.index.get(key);
        }finally {
            indexLock.readLock().unlock();
        }
    }

    private void loadIndexFile() throws FileNotFoundException {
        JsonReader jreader = new JsonReader(
                new FileReader(this.indexPath.toAbsolutePath().toString())
        );
        Gson gson = IOUtils.getGson();
        index = gson.fromJson(jreader, new TypeToken<Map<String, IndexItem>>(){}.getType());
    }
}
