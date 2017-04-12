package it.com.youzan.filebackup;

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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/4/11.
 */
public class BackupScopeTest {
    private static final Logger logger = LoggerFactory.getLogger(BackupScopeTest.class);

    @Test
    public void testRandomWriteAndRead() throws IOException, InterruptedException {
        BackupScopeConfig config = new BackupScopeConfig();
        config.setBackupFileMaxByte(1000);
        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testRandomWriteAndRead")
                .setBackupContext(new DefaultBackupContext("testRandomWriteAdnRead"))
                .setBackupScopeConfig(config)
                .build();
        aScope.init();
        aScope.openRead();
        aScope.openWrite();
        final CountDownLatch latch = new CountDownLatch(1);
        final int num = 1000;
        final AtomicInteger cnt = new AtomicInteger(0);
        ExecutorService readExec = Executors.newSingleThreadExecutor();
        readExec.submit((Runnable)()->{
            while(true) {
                try {
                    byte[] contents = aScope.tryRead();
                    if(null != contents)
                        logger.info("Total read {}, {}", cnt.incrementAndGet(), new String(contents));
                    else{
                        Thread.sleep(500L);
                    }
                    if(null != contents && num == cnt.get()){
                        aScope.closeRead();
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        });

        ExecutorService writeExec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for(int idx = 0; idx < num; idx++) {
            final int i = idx;
            writeExec.submit((Runnable) () -> {
                try {
                    aScope.tryWrite(("this is " + i).getBytes(Charset.defaultCharset()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        aScope.closeWrite();
        clear(Paths.get("src/test/resources/testRandomWriteAndRead"));
    }

    private void clear(Path path) throws IOException {
        DirectoryDelete walk = new DirectoryDelete();
        EnumSet opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(path, opts, Integer.MAX_VALUE, walk);
    }
}
