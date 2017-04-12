package com.youzan.filebackup.context;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * BackupScopeBuilder to help setup a scope builder
 * Created by lin on 17/4/7.
 */
public class BackupScopeBuilder {
    private final BackupScope nestedScope;

    BackupScopeBuilder(final Path scopePath) {
        nestedScope = new BackupScope(scopePath);
    }

    BackupScopeBuilder(final Path scopePath, String scopeId) {
        nestedScope = new BackupScope(scopePath, scopeId);
    }

    public static BackupScopeBuilder create(String scopePath) {
        Path path = Paths.get(scopePath);
        return new BackupScopeBuilder(path);
    }

    public static BackupScopeBuilder create(String scopePath, String scopeId) {
        Path path = Paths.get(scopePath);
        return new BackupScopeBuilder(path, scopeId);
    }

    public BackupScopeBuilder setBackupContext(final BackupContext backupContext) {
        this.nestedScope.setBackupContext(backupContext);
        return this;
    }

    public BackupScopeBuilder setBackupScopeConfig(final BackupScopeConfig config) {
        this.nestedScope.setBackupScopeConfig(config);
        return this;
    }

    public BackupScope build() {
        return this.nestedScope;
    }
}
