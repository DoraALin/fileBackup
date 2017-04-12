package com.youzan.filebackup.files;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.youzan.filebackup.context.BackupScope;
import com.youzan.filebackup.context.BackupScopeBuilder;
import com.youzan.filebackup.context.DefaultBackupContext;
import com.youzan.filebackup.utils.DirectoryDelete;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lin on 17/4/7.
 */
public class IndexFileTest {

    @Test
    public void testLoadIndexFile() throws IOException {
        //set up index map and serialize it into test resources
        Path idxPath = Paths.get("./src/test/resources/testLoadIndexFile/scope.idx").toAbsolutePath();
        Files.createDirectories(idxPath.getParent());
        Files.createFile(idxPath);
        Map<String, BackupIndex.IndexItem> map = new ConcurrentHashMap<>();
        map.put("key0", new BackupIndex.IndexItem(0, 0, 0));
        map.put("key1", new BackupIndex.IndexItem(1, 1, 1));
        map.put("key2", new BackupIndex.IndexItem(2, 2, 2));
        Gson gson = new Gson();
        JsonWriter jwriter = new JsonWriter(
                new FileWriter(idxPath.toString())
        );
        gson.toJson(map, map.getClass(), jwriter);
        jwriter.close();

        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testLoadIndexFile")
        .setBackupContext(new DefaultBackupContext("testLoadIndexFileContext"))
        .build();

        BackupIndex index = new BackupIndex(aScope);

        BackupIndex.IndexItem item0 = index.lookup("key0");
        Assert.assertNotNull(item0);
        Assert.assertEquals(0L, item0.getLength());
        Assert.assertEquals(0L, item0.getBackupFileIndex());
        Assert.assertEquals(0L, item0.getOffset());

        BackupIndex.IndexItem item1 = index.lookup("key1");
        Assert.assertEquals(1L, item1.getLength());
        Assert.assertEquals(1L, item1.getBackupFileIndex());
        Assert.assertEquals(1L, item1.getOffset());

        BackupIndex.IndexItem item2 = index.lookup("key2");
        Assert.assertEquals(2L, item2.getLength());
        Assert.assertEquals(2L, item2.getBackupFileIndex());
        Assert.assertEquals(2L, item2.getBackupFileIndex());
        clear(Paths.get("src/test/resources/testLoadIndexFile"));
    }

    private void clear(Path path) throws IOException {
        DirectoryDelete walk = new DirectoryDelete();
        EnumSet opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(path, opts, Integer.MAX_VALUE, walk);
    }
}
