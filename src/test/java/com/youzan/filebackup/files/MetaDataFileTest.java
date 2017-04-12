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

/**
 * Created by lin on 17/4/10.
 */
public class MetaDataFileTest {
    @Test
    public void testLoadMetaDataFile() throws IOException {
        //set up neta data instance and serialize it into test resources
        Path metaPath = Paths.get("./src/test/resources/testLoadMetaFile/scope.meta").toAbsolutePath();
        Files.createDirectories(metaPath.getParent());
        Files.createFile(metaPath);
        BackupMeta.BackupMetaInfo metaInfo = new BackupMeta.BackupMetaInfo(new BackupLocation(1L, 1L),
                new BackupLocation(2L, 2L),
                new BackupLocation(3L, 3L));

        Gson gson = new Gson();
        JsonWriter jwriter = new JsonWriter(
                new FileWriter(metaPath.toString())
        );
        gson.toJson(metaInfo, metaInfo.getClass(), jwriter);
        jwriter.close();

        BackupScope aScope = BackupScopeBuilder.create("src/test/resources/", "testLoadMetaFile")
                .setBackupContext(new DefaultBackupContext("testLoadMetaFileContext"))
                .build();

        BackupMeta meta = new BackupMeta(aScope);
        Assert.assertEquals(1L, meta.getReadStart().getBackupFileIndex());
        Assert.assertEquals(2L, meta.getReadEnd().getBackupFileOffset());
        Assert.assertEquals(3L, meta.getWriteStart().getBackupFileIndex());

        clear(Paths.get("src/test/resources/testLoadMetaFile"));
    }

    private void clear(Path path) throws IOException {
        DirectoryDelete walk = new DirectoryDelete();
        EnumSet opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(path, opts, Integer.MAX_VALUE, walk);
    }
}
