package com.youzan.filebackup;

import com.youzan.filebackup.context.BackupScope;
import com.youzan.filebackup.context.BackupScopeBuilder;
import com.youzan.filebackup.context.BackupScopeConfig;
import com.youzan.filebackup.context.DefaultBackupContext;
import com.youzan.filebackup.utils.DirectoryDelete;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/4/10.
 */
public class BackupScopeTest {
    private final static Logger logger = LoggerFactory.getLogger(BackupScopeTest.class);
    @Test
    public void testBackupScopeInit() throws IOException {
        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testBackupScopeInit")
                .setBackupContext(new DefaultBackupContext("testBackupScopeInitContext"))
                .build();
        aScope.init();
        aScope.openRead();
        aScope.openWrite();
        aScope.closeRead();
        aScope.couldWrite();
        clear(Paths.get("src/test/resources/testBackupScopeInit"));
    }

    @Test
    public void testBackupScopeBasicWrite() throws IOException {
        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testBackupScopeBasicWrite")
                .setBackupContext(new DefaultBackupContext("testBackupScopeBasicWrite"))
                .build();
        aScope.init();
        aScope.openRead();
        aScope.openWrite();
        byte[] contents = "there is content".getBytes(Charset.defaultCharset());
        aScope.tryWrite(contents);
        aScope.closeWrite();
        aScope.closeRead();
        clear(Paths.get("src/test/resources/testBackupScopeBasicWrite"));
    }

    @Test
    public void testBackupFileUpdate() throws IOException {
        BackupScopeConfig config = new BackupScopeConfig();
        config.setBackupFileMaxByte(10);
        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testBackupFileUpdate")
                .setBackupContext(new DefaultBackupContext("testBackupFileUpdate"))
                .setBackupScopeConfig(config)
                .build();
        aScope.init();
        aScope.openWrite();

        byte[] contents = "testBackupFileUpdate testBackupFileUpdate".getBytes(Charset.defaultCharset());
        aScope.tryWrite(contents);
        aScope.tryWrite(contents);
        //scope should open third backup file for write. Also check meta-dat file
        Assert.assertEquals(2, aScope.getBackupMetaInfo().getWriteStart().getBackupFileIndex());
        aScope.closeWrite();
        clear(Paths.get("src/test/resources/testBackupFileUpdate"));
    }

    @Test
    public void testReadAfterWrite() throws IOException {
        BackupScopeConfig config = new BackupScopeConfig();
        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testReadAfterWrite")
                .setBackupContext(new DefaultBackupContext("testReadAfterWrite"))
                .setBackupScopeConfig(config)
                .build();
        aScope.init();
        aScope.openWrite();
        aScope.openRead();

        byte[] content1 = "testReadAfterWriteFirst".getBytes(Charset.defaultCharset());
        long len1 = content1.length;
        aScope.tryWrite(content1);
        byte[] content2 = "testReadAfterWriteSecond".getBytes(Charset.defaultCharset());
        long len2 = content2.length;
        aScope.tryWrite(content2);

        //open red again, should succeed
        byte[] contRead1 = aScope.tryRead();
        Assert.assertEquals(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE + BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE + len1, aScope.getBackupMetaInfo().getReadStart().getBackupFileOffset());

        byte[] contRead2 = aScope.tryRead();
        Assert.assertEquals(BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE + BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE * 2  + len1 + len2, aScope.getBackupMetaInfo().getReadStart().getBackupFileOffset());

        String contRead1Str = new String(contRead1);
        String contRead2Str = new String(contRead2);

        aScope.closeRead();
        aScope.closeWrite();
        clear(Paths.get("src/test/resources/testReadAfterWrite"));
    }

    @Test
    public void testBackupMetaUpdateAfterBackupScopeBasicWrite() throws IOException, InterruptedException {
        int nThread = Runtime.getRuntime().availableProcessors() * 2;
        ExecutorService exec = Executors.newFixedThreadPool(nThread);
        final BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testBackupMetaUpdateAfterBackupScopeBasicWrite")
                .setBackupContext(new DefaultBackupContext("testBackupMetaUpdateAfterBackupScopeBasicWrite"))
                .build();
        aScope.init();
        aScope.openRead();
        aScope.openWrite();
        final CountDownLatch latch = new CountDownLatch(nThread);
        final AtomicLong total = new AtomicLong(0);
        for(int i = 0; i < nThread; i++) {
            byte[] content = ("this is " + i).getBytes(Charset.defaultCharset());
            exec.submit(() -> {
                try {
                    aScope.tryWrite(content);
                    total.addAndGet(BackupScopeConfig.BACKUP_ITEM_MAX_SIZE_IN_BYTE + content.length);
                    latch.countDown();
                } catch (IOException e) {
                    logger.error("write fail.");
                }
            });
        }
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        Assert.assertEquals(total.get() + BackupScopeConfig.BACKUP_FILE_MAX_SIZE_IN_BYTE, aScope.getBackupMetaInfo().getWriteStart().getBackupFileOffset());
        aScope.closeRead();
        aScope.closeWrite();
        clear(Paths.get("src/test/resources/testBackupMetaUpdateAfterBackupScopeBasicWrite"));
    }

    private void clear(Path path) throws IOException {
        DirectoryDelete walk = new DirectoryDelete();
        EnumSet opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(path, opts, Integer.MAX_VALUE, walk);
    }
}
