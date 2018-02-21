package simpledb;

import org.apache.mina.util.ConcurrentHashSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This project is using page level locking only. So the lock manager implements page level locking.
 * <p>
 * This class maintains info :
 * 1. locks being held on a page
 * 2.
 */
public class LockManager {
    private Map<PageId, ReentrantReadWriteLock> hash;
    private Map<TransactionId, Set<ReentrantReadWriteLock.ReadLock>> readLockTable;
    private Map<TransactionId, Set<ReentrantReadWriteLock.WriteLock>> writeLockTable;

    public LockManager() {
        hash = new ConcurrentHashMap<>();
        readLockTable = new ConcurrentHashMap<>();
        writeLockTable = new ConcurrentHashMap<>();
    }

    public void acquireSharedLock(TransactionId tid, PageId pageId) {
        ReentrantReadWriteLock lock = hash.computeIfAbsent(pageId, k -> new ReentrantReadWriteLock());
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        //mark tid with pid
        Set<ReentrantReadWriteLock.ReadLock> s = readLockTable.computeIfAbsent(tid, k -> new ConcurrentHashSet<>());
        s.add(readLock);
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pageId) {
        if (isTransactionHoldsReadLockOnPage(tid, pageId)) {
            upgradeSharedLockToExclusiveLock(tid, pageId);
            return;
        }
        ReentrantReadWriteLock lock = hash.computeIfAbsent(pageId, k -> new ReentrantReadWriteLock());
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        //mark this
        Set<ReentrantReadWriteLock.WriteLock> s = writeLockTable.computeIfAbsent(tid, k -> new ConcurrentHashSet<>());
        s.add(writeLock);
    }

    public void upgradeSharedLockToExclusiveLock(TransactionId tid, PageId pageId) {
        ReentrantReadWriteLock lock = hash.get(pageId);
        assert lock != null;
        Set<ReentrantReadWriteLock.ReadLock> rs = readLockTable.get(tid);
        assert rs != null;
        assert rs.contains(lock.readLock());
        readLockTable.remove(tid);
        lock.readLock().unlock();
        acquireExclusiveLock(tid, pageId);
    }

    public void releaseLock(TransactionId tid, PageId pageId) {
        ReentrantReadWriteLock readWriteLock = hash.get(pageId);
        assert readWriteLock != null;
        Set<ReentrantReadWriteLock.ReadLock> rs = readLockTable.get(tid);
        if (rs != null && rs.contains(readWriteLock.readLock())) {
            readWriteLock.readLock().unlock();
            return;
        }
        Set<ReentrantReadWriteLock.WriteLock> ws = writeLockTable.get(tid);
        if (ws != null && ws.contains(readWriteLock.writeLock())) {
            readWriteLock.writeLock().unlock();
        }
    }

    public boolean isTransactionHoldsALockOnPage(TransactionId tid, PageId pageId) {
        return isTransactionHoldsReadLockOnPage(tid, pageId) || isTransactionHoldsWriteLockOnPage(tid, pageId);
    }

    public boolean isTransactionHoldsReadLockOnPage(TransactionId tid, PageId pageId) {
        ReentrantReadWriteLock readWriteLock = hash.get(pageId);
        if (readWriteLock == null) {
            return false;
        }
        Set<ReentrantReadWriteLock.ReadLock> rs = readLockTable.get(tid);
        if (rs != null && rs.contains(readWriteLock.readLock())) {
            return true;
        }
        return false;
    }

    public boolean isTransactionHoldsWriteLockOnPage(TransactionId tid, PageId pageId) {
        ReentrantReadWriteLock readWriteLock = hash.get(pageId);
        if (readWriteLock == null) {
            return false;
        }
        Set<ReentrantReadWriteLock.WriteLock> ws = writeLockTable.get(tid);
        if (ws != null && ws.contains(readWriteLock.writeLock())) {
            return true;
        }
        return false;
    }

    public void releaseAllLock(TransactionId tid) {
        Set<ReentrantReadWriteLock.ReadLock> rs = readLockTable.get(tid);
        if (rs != null) {
            for (ReentrantReadWriteLock.ReadLock lock : rs) {
                lock.unlock();
            }
        }
        Set<ReentrantReadWriteLock.WriteLock> ws = writeLockTable.get(tid);
        if (ws != null) {
            for (ReentrantReadWriteLock.WriteLock lock : ws) {
                lock.unlock();
            }
        }
    }
}
