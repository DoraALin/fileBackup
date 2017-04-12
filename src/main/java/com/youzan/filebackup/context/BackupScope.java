package com.youzan.filebackup.context;

import com.youzan.filebackup.files.BackupIndex;
import com.youzan.filebackup.files.BackupLocation;
import com.youzan.filebackup.files.BackupMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lin on 17/4/7.
 */
public class BackupScope {
    private final static Logger logger = LoggerFactory.getLogger(BackupScope.class);
    private final static String SCOPE_FOLDER_FORMAT = "backup_scope_%s";
    private final static String SCOPE_INDEX_FILE_NAME = "scope.idx";
    private final static String SCOPE_META_FILE_NAME = "scope.meta";
    private final static String SCOPE_BACKUP_FILE_NAME = "scope.backup_%d";

    //TODO resources need shutdown
    private final ExecutorService writeExec = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService readExec = Executors.newSingleThreadScheduledExecutor();

    private final Path parent;
    //initialize with default backup scope config
    private BackupScopeConfig config = new BackupScopeConfig();
    //scope id, if there is already a scope folder in passin path, then pickup existing
    private String scopeId;
    //backup context which current scope belongs to
    private BackupContext backupContext = null;
    private BackupIndex index;
    private BackupMeta metaData;
    private final AtomicInteger state = new AtomicInteger(Status.READY.ordinal());

    private volatile boolean read = false;
    private ReentrantReadWriteLock readChannelLock = new ReentrantReadWriteLock();
    private final Object syncReadChannel = new Object();
    private FileChannel readFileChannel = null;
    private long readFileMaxSize;
    private FileLock readLock;

    private volatile boolean write = false;
    private ReentrantReadWriteLock writeChannelLock = new ReentrantReadWriteLock();
    private final Object syncWriteChannel = new Object();
    private FileChannel writeFileChannel = null;
    private long writeFileMaxSize;
    private FileLock writeLock;

    enum Status {
        READY,
        IN_INIT,
        INIT,
        INVALID,
    }

    BackupScope(Path parent) {
        if(null == parent)
            throw new IllegalArgumentException("Scope path could not be null.");
        this.parent = parent;
        scopeId = String.format(SCOPE_FOLDER_FORMAT, UUID.randomUUID());
    }

    BackupScope(Path parent, String scopeId) {
        if(null == parent)
            throw new IllegalArgumentException("Scope parent path could not be null.");
        if(null == scopeId || scopeId.isEmpty())
            throw new IllegalArgumentException("Scope Id passin could not be null.");
        this.parent = parent;
        this.scopeId = scopeId;
    }

    /**
     * Set {@link BackupScopeConfig} for current backup scope. Function need to be invoked BEFORE init() is invoked.
     * @param config    backup scope
     */

    void setBackupScopeConfig(BackupScopeConfig config) {
       this.config = config;
    }

    void setBackupContext(final BackupContext backupcxt) {
        if(null != this.backupContext)
            throw new IllegalStateException("Backup context already existed in current back up scope.");
        if(null == backupcxt)
            throw new IllegalArgumentException("Backup context could not be null.");
        this.backupContext = backupcxt;
        backupcxt.addScope(this);
    }

    public Path getParent() {
        return this.parent;
    }

    public String getScopeId() {
        return this.scopeId;
    }

    /**
     * return index file path of current backup scope
     * @return  index file path
     */
    public Path getIndexPath() {
        return this.parent.resolve(this.scopeId)
                .resolve(SCOPE_INDEX_FILE_NAME);
    }

    /**
     * return meta-data file path of current backup scope
     * @return meta-data file path
     */
    public Path getMetaPath() {
        return this.parent.resolve(this.scopeId)
                .resolve(SCOPE_META_FILE_NAME);
    }

    public String toString(){
        return this.scopeId + "@" + this.parent;
    }

    public int hashCode() {
        return this.scopeId.hashCode();
    }

    /**
     * initialize current backup scope
     */
    public void init() {
        if(!state.compareAndSet(Status.READY.ordinal(), Status.IN_INIT.ordinal()))
            return;
        Path scopePath = this.parent.resolve(this.scopeId);
        if(isScopeExist(scopePath)){
            logger.info("Backup scope path: {} exists.", scopePath);
        } else {
            //create new
            try {
                Files.createDirectory(scopePath);
            } catch (IOException e) {
                logger.error("Could not initialize scope directory {}.", parent, e);
            }
        }
        index = new BackupIndex(this);
        metaData = new BackupMeta(this);
        state.compareAndSet(Status.IN_INIT.ordinal(), Status.INIT.ordinal());
        logger.info("BackupScope {} initialized.", this);
    }

    private boolean isScopeExist(final Path scopePath) {
        //check if there is same scope id in parent folder
        return Files.exists(this.parent.resolve(scopeId));
    }

    /**
     * Open current backup scope for I/O. open operation open channel to target backup file, according to read/write
     * start in meta-data file.
     */
    public boolean openWrite() throws IOException {
        if(state.get() != Status.INIT.ordinal())
            return false;

        //for write
        BackupLocation writeStartBackupFileLoc = metaData.getWriteStart();
        Path writeBackupFilePath = this.parent.resolve(this.scopeId).resolve(String.format(SCOPE_BACKUP_FILE_NAME, writeStartBackupFileLoc.getBackupFileIndex()));
        boolean shouldRecordMaxSize = false;
        if(Files.exists(writeBackupFilePath))
            shouldRecordMaxSize = true;

        try {
            writeFileChannel = FileChannel.open(writeBackupFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            long position = writeFileChannel.position();
            if(shouldRecordMaxSize) {
                logger.info("Record current position {} in existing backup file for writing.", position);
                writeFileChannel.position(0);
                ByteBuffer maxSizeBuf = ByteBuffer.allocateDirect(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE);
                writeFileChannel.read(maxSizeBuf);
                this.writeFileMaxSize = maxSizeBuf.getLong();
                maxSizeBuf.clear();
                writeFileChannel.position(position);
                logger.info("Current backup file max size, for write {}", this.writeFileMaxSize);
            }else{
                //write max size in config into new created backup file
                ByteBuffer maxSizeBuf = ByteBuffer.allocateDirect(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE);
                maxSizeBuf.putLong(this.config.getBackupFileMaxByte());
                maxSizeBuf.flip();
                writeFileChannel.write(maxSizeBuf);
                this.writeFileMaxSize = this.config.getBackupFileMaxByte();
                maxSizeBuf.clear();
                updateEnd(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE);
                logger.info("Write max backup file size {} into newly created file.", this.config.getBackupFileMaxByte());
            }
        } catch (IOException e) {
            logger.error("Fail to open write backup file {}.", writeBackupFilePath, e);
            state.set(Status.INVALID.ordinal());
            throw e;
        }

        updateWriteLock();
        logger.info("Backup file {} open for write.", writeBackupFilePath);
        //update status
        this.write = true;
        return this.write;
    }

    private void updateWriteLock() throws IOException {
        BackupLocation writeStartBackupFileLoc = metaData.getWriteStart();
        Path writeBackupFilePath = this.parent.resolve(this.scopeId).resolve(String.format(SCOPE_BACKUP_FILE_NAME, writeStartBackupFileLoc.getBackupFileIndex()));
        try {
            writeChannelLock.writeLock().lock();
            if(null != writeLock && writeLock.isValid()) {
                writeLock.release();
//                logger.info("Write lock released for {}", writeBackupFilePath);
            }
            long writeBackupFileOffset = writeStartBackupFileLoc.getBackupFileOffset();
            if ((writeLock = writeFileChannel.tryLock(writeBackupFileOffset, Long.MAX_VALUE - writeBackupFileOffset, false)) == null) {
                state.set(Status.INVALID.ordinal());
                throw new AccessDeniedException("Fail to acquire write lock on backup file " + writeBackupFilePath);
            }
        }finally {
            writeChannelLock.writeLock().unlock();
        }
    }

    public synchronized void closeRead() throws IOException {
        if(!couldRead()) {
            logger.info("Backup read is already closed.");
            return;
        }
        try{
            read = false;
            this.readFileChannel.close();
            logger.info("Backup file {} closed.", this.metaData.getReadStart());
            //persist meta data file
            this.metaData.commitMetaFile();
        } catch (IOException e) {
            logger.error("Fail to close backup file {}.", this.metaData.getReadStart(), e);
        } finally {
            if(this.readLock.isValid())
                this.readLock.release();
        }
    }

    public synchronized void closeWrite() throws IOException {
        if(!couldWrite()) {
            logger.info("Backup write is already closed.");
            return;
        }
        try {
            write = false;
            this.writeFileChannel.close();
            logger.info("Backup file {} closed.", this.metaData.getWriteStart());
            this.metaData.commitMetaFile();
        } catch (IOException e) {
            logger.error("Fail to close backup file {}.", this.metaData.getWriteStart(), e);
        } finally {
            if(this.writeLock.isValid())
                this.writeLock.release();
        }
    }

    /**
     * read next content from back up file
     * @return  content in byte array
     */
    private byte[] readOne() throws IOException {
        if(!couldRead())
            return null;
        //read content size
        ByteBuffer buf = ByteBuffer.allocateDirect(BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE);
        readFileChannel.read(buf);
        buf.position(0);
        int length = buf.getInt();
        buf.clear();
        buf = ByteBuffer.allocateDirect(length);
        readFileChannel.read(buf);
        byte[] content = new byte[length];
        buf.position(0);
        buf.get(content);
        buf.clear();
        return content;
    }

    /**
     * Read one content from current backup file
     * @return byte[] array
     */
    public byte[] tryRead() throws IOException {
        synchronized(syncReadChannel){
            if(!couldRead() && !openRead())
                return null;

            BackupLocation readStartLoc = metaData.getReadStart();
            BackupLocation readEndLoc = metaData.getReadEnd();
            if(!BackupLocation.hasOffset(readStartLoc, readEndLoc))
                return null;
            byte[] content = null;
            try {
                content = readOne();
            } catch (IOException e) {
                logger.error("Fail to read backup file {}.", metaData.getReadStart());
            }

            if(null != content) {
                long newReadOffset = readStartLoc.getBackupFileOffset() + BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE + content.length;
                //update read start
                metaData.setReadStart(new BackupLocation(readStartLoc.getBackupFileIndex(), newReadOffset));
                //open next backup file, if read start offset reaches position and write start in another backup file
                if(this.readFileMaxSize < newReadOffset && readEndLoc.getBackupFileIndex() > readStartLoc.getBackupFileIndex()) {
                    logger.info("Backup file reached end of {}, file size {}", readStartLoc, this.readFileMaxSize);
                    metaData.setReadStart(new BackupLocation(readStartLoc.getBackupFileIndex() + 1, 0));
                    //open next backup file for read
                    closeRead();
                    openRead();
                } else {
                    //update lock lock, including read start and read end(updated after write)
                    updateReadLock();
                }
            }else{
                logger.info("Read nothing from {}", readStartLoc);
            }
            return content;
        }
    }

    public boolean openRead() throws IOException {
        if(state.get() != Status.INIT.ordinal())
            return false;

        //initialize read offset and
        BackupLocation readEndBackupFileLoc = metaData.getReadEnd();
        BackupLocation readStartBackupFileLoc = metaData.getReadStart();
        //check if there is need to create read
        if(!BackupLocation.hasOffset(readStartBackupFileLoc, readEndBackupFileLoc)){
            logger.info("There is no existing backup content to read. openRead exits.");
            return false;
        }

        Path readBackupFilePath = this.parent.resolve(this.scopeId).resolve(String.format(SCOPE_BACKUP_FILE_NAME, readStartBackupFileLoc.getBackupFileIndex()));
        try {
            readFileChannel = FileChannel.open(readBackupFilePath, StandardOpenOption.READ);
        } catch (IOException e) {
            logger.error("Fail to open read backup file {}.", readBackupFilePath, e);
            state.set(Status.INVALID.ordinal());
            throw  e;
        }
        ByteBuffer maxSizeBuf = ByteBuffer.allocateDirect(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE);
        readFileChannel.read(maxSizeBuf);
        maxSizeBuf.position(0);
        this.readFileMaxSize = maxSizeBuf.getLong();
        logger.info("Set backup file max size {} for {}", this,readFileMaxSize, readBackupFilePath);
        maxSizeBuf.clear();

        long newReadOffset = readStartBackupFileLoc.getBackupFileOffset() + BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE;
        //update read start
        metaData.setReadStart(new BackupLocation(readStartBackupFileLoc.getBackupFileIndex(), newReadOffset));

        updateReadLock();
        readFileChannel.position(metaData.getReadStart().getBackupFileOffset());
        logger.info("Backup file {} open for read.", readBackupFilePath);
        //update status
        read = true;
        return read;
    }

    private void updateReadLock() throws IOException {
        BackupLocation readEndBackupFileLoc = metaData.getReadEnd();
        BackupLocation readStartBackupFileLoc = metaData.getReadStart();
        Path readBackupFilePath = this.parent.resolve(this.scopeId).resolve(String.format(SCOPE_BACKUP_FILE_NAME, readStartBackupFileLoc.getBackupFileIndex()));
        Long readLockEnd = Long.MAX_VALUE - 1;
        long readStartBackupFileOffset = readStartBackupFileLoc.getBackupFileOffset();
        if(readEndBackupFileLoc.getBackupFileIndex() == readStartBackupFileLoc.getBackupFileIndex())
            readLockEnd = readEndBackupFileLoc.getBackupFileOffset();
        try {
            readChannelLock.writeLock().lock();
            if(null != readLock && readLock.isValid())
                readLock.release();
            if ((readLock = readFileChannel.tryLock(readStartBackupFileOffset, readLockEnd - readStartBackupFileOffset + 1, true)) == null) {
                state.set(Status.INVALID.ordinal());
                throw new AccessDeniedException("Fail to acquire read lock on backup file " + readBackupFilePath);
            }
        } finally {
            readChannelLock.writeLock().unlock();
        }
    }

    public boolean couldWrite() {
        return write;
    }

    public boolean couldRead() {
        return read;
    }

    /**
     * Write byte array content into backup file.
     * @param contents  content in byte
     * @return byte write
     */
    private int writeBytes(final byte[] contents, long pos) throws IOException {
        if(!couldWrite())
            return 0;
        //create buffer according to BackupScope config
        ByteBuffer buf = ByteBuffer.allocateDirect(contents.length + BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE);
        buf.putInt(contents.length);
        buf.put(contents);
        buf.flip();
        try {
            return writeFileChannel.write(buf, pos);
        }finally {
            buf.clear();
        }
    }

    /**
     * Write bytes array into current backup scope.
     * @param contents bytes array to write
     * @return byte write count
     */
    public synchronized int tryWrite(final byte[] contents) throws IOException {
        synchronized (syncWriteChannel){
            if(!couldWrite() && !openWrite())
                return 0;
            //1. write file
            int count;
            try {
                count = writeBytes(contents, this.metaData.getWriteStart().getBackupFileOffset());
            } catch (IOException e) {
                logger.error("Fail to write to backup file.");
                throw e;
            }
            updateEnd(count);//update meta info in memory
            return count;
        }
    }

    /**
     * Update write end and read end
     * @param count byte count
     */
    private void updateEnd(int count) throws IOException {
        long backupFileIndex = this.metaData.getWriteStart().getBackupFileIndex();
        long backupFileOffset = this.metaData.getWriteStart().getBackupFileOffset();
        if (count > 0) {
            //check if we need to create another backup file
            if((backupFileOffset + count) > this.writeFileMaxSize) {
                //update meta data in memory
                this.metaData.setWriteStart(new BackupLocation(++backupFileIndex, 0));
                closeWrite();
                //open write should lock new backup file
                openWrite();
            } else {
                this.metaData.setWriteStart(new BackupLocation(backupFileIndex, backupFileOffset + count));
                //write lock update
                updateWriteLock();
            }
            //update read end without updating read lock
            this.metaData.setReadEnd(new BackupLocation(backupFileIndex, backupFileOffset + count - 1));
        }
    }

    /**
     * Async write to backup file, function answers with {@link Future<Integer>} for future byte count.
     * @param contents
     * @return  future
     * @throws RejectedExecutionException
     */
    public Future<Integer> writeAsync(final byte[] contents) throws RejectedExecutionException {
        return writeExec.submit((Callable<Integer>) () -> {
            int count = tryWrite(contents);
            return count;
        });
    }

    public BackupMeta getBackupMetaInfo() {
        return this.metaData;
    }
}
