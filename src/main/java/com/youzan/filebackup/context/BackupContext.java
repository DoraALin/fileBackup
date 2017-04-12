package com.youzan.filebackup.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Context which has multi back up scopes within.
 * Created by lin on 17/4/7.
 */
abstract public class BackupContext {
    private final static Logger logger = LoggerFactory.getLogger(BackupContext.class);
    final static String CONTEXT_ID_FORMAT = "backup_context_%s";
    //context name
    private final String name;
    private final String contextId;
    private final ReentrantReadWriteLock scopesLock = new ReentrantReadWriteLock();
    private final Set<BackupScope> scopes;

    public BackupContext(String name) {
        if(null == name || name.isEmpty())
            throw new IllegalArgumentException("Backup context could not be null.");
        this.name = name;
        contextId = String.format(CONTEXT_ID_FORMAT, UUID.randomUUID());
        scopes = new HashSet<>();
    }

    void addScope(final BackupScope scope) {
        try{
            scopesLock.writeLock().lock();
            if(scopes.contains(scope))
                return;
            scopes.add(scope);
            logger.info("Backup scope {} added in current context {}.", scope, this);
        }finally {
            scopesLock.writeLock().unlock();
        }

    }

    public String toString() {
        return this.name + ":" + this.contextId;
    }
}
