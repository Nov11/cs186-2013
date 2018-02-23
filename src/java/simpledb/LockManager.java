package simpledb;

import org.apache.mina.util.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This project is using page level locking only. So the lock manager implements page level locking.
 * <p>
 * Test cases requires that a single transaction can be running in different threads.
 * Read Write lock is not applicable as it is not allowed to do unlock from a different thread.
 * I have to implement a more customized version of lock with the functions:
 * 1. read lock & write lock
 * 2. block & wake up
 * 3. upgrade from read lock to write lock if possible
 * 4. check tid related to current active lock type. i.e. for read lock being hold return the transaction set
 * 5. very common that tid already lock on a page and request same kind of lock later, since this is page level locking.
 * 6. it's able to get a read lock if itself holds a write lock.
 * (emm.... guess I'm not very fond of page level locking
 * <p>
 * About lock manager
 * This class maintains info :
 * 1. locks related to a page(there is no page deletion function for now, or lock manager should be updated)
 * 2. transactions to their locks being held
 * <p>
 * java.util's read write lock version is listed in the bottom but not used.
 * </p>
 */
public class LockManager {
    //    private ReentrantLock mutex;
    private Map<PageId, CustomLock> hash;
    private Map<TransactionId, Set<CustomLock>> tid2Lock;


    public LockManager() {
        hash = new ConcurrentHashMap<>();
//        mutex = new ReentrantLock();
        tid2Lock = new ConcurrentHashMap<>();
    }

    public void acquireSharedLock(TransactionId tid, PageId pageId) throws TransactionAbortedException {
        CustomLock lock = hash.computeIfAbsent(pageId, k -> new CustomLock());
        lock.lockRead(tid);
        tid2Lock.computeIfAbsent(tid, k -> new ConcurrentHashSet<>()).add(lock);
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pageId) throws TransactionAbortedException {
        CustomLock lock = hash.computeIfAbsent(pageId, k -> new CustomLock());
        lock.lockWrite(tid);
        tid2Lock.computeIfAbsent(tid, k -> new ConcurrentHashSet<>()).add(lock);
    }
// no need
//    public void upgradeSharedLockToExclusiveLock(TransactionId tid, PageId pageId) {
//
//    }

    public void releaseLock(TransactionId tid, PageId pageId) {
        CustomLock lock = hash.computeIfAbsent(pageId, k -> new CustomLock());
        lock.releaseLock(tid);
        tid2Lock.get(tid).remove(lock);
    }

    public boolean isTransactionHoldsALockOnPage(TransactionId tid, PageId pageId) {
        CustomLock lock = hash.computeIfAbsent(pageId, k -> new CustomLock());
        return lock.isHoldingLock(tid);
    }

    public void releaseAllLocks(TransactionId tid) {
        Set<CustomLock> s = tid2Lock.get(tid);
        //There must be something insane going on if releaseAllLocks is invoked in more than one thread.
        if (s == null) {
            return;
        }
        for (CustomLock lock : s) {
            lock.releaseLock(tid);
        }
        tid2Lock.remove(tid);
    }
}

class CustomLock {
    private Set<TransactionId> holdingLock; //transactions that hold lock on this object
    //it's possible that readCount and writeCount both equals to 1
    //that'll be the case when transaction request a write lock and later a read lock on the same page.
    private int readCount = 0; //number of read locks
    private int writeCount = 0;//number of write locks
    private TransactionId waitPending;

    public CustomLock() {
        holdingLock = new HashSet<>();
    }

    public synchronized void lockRead(TransactionId tid) throws TransactionAbortedException {
        long waitTime = 5;
        boolean timeOut = false;
        while (!(writeCount == 0) && !(writeCount == 1 && holdingLock.contains(tid)) && !timeOut) {
            long cur = System.currentTimeMillis();
            try {
                wait(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long prev = cur;
            cur = System.currentTimeMillis();
            waitTime -= cur - prev;
            timeOut = waitTime < 0;
        }
        if (timeOut) {
            throw new TransactionAbortedException(tid.getId() + "");
        }
        if (writeCount == 0) {
            if (!holdingLock.contains(tid)) {
                readCount++;
                holdingLock.add(tid);
            }
        } else if (writeCount == 1 && holdingLock.contains(tid)) {
            if (readCount == 0) {
                readCount = 1;
            }
        } else {
            assert false;
        }
    }

    public synchronized void lockWrite(TransactionId tid) throws TransactionAbortedException {
        final int WAIT_TIME = 200;
        long waitTime = WAIT_TIME;
        boolean timeOut = false;
        while (!(writeCount == 0 && readCount == 0)
                && !(writeCount == 0 && readCount == 1 && holdingLock.contains(tid))
                && !(writeCount == 1 && holdingLock.contains(tid))
                && !timeOut) {
            if (waitPending == null) {
                waitPending = tid;
            } else if (tid.getId() > waitPending.getId()) {
                throw new TransactionAbortedException(tid.getId() + "");
            } else if (tid.getId() < waitPending.getId()) {
                waitPending = tid;
                notifyAll();
            }
            long cur = System.currentTimeMillis();
            try {
                wait(waitTime+1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long prev = cur;
            cur = System.currentTimeMillis();
            waitTime -= cur - prev;
            timeOut = waitTime <= 0;
        }
        if (timeOut) {
            if (waitPending == tid) {
                waitPending = null;
            }
            throw new TransactionAbortedException(tid.getId() + "");
        }
        if (readCount == 0 && writeCount == 0) {
            assert holdingLock.size() == 0;
            writeCount++;
            holdingLock.add(tid);
        } else if (readCount == 1 && writeCount == 0 && holdingLock.contains(tid)) {
            writeCount++;
        } else if (writeCount == 1 && holdingLock.contains(tid)) {
            assert readCount == 0 || readCount == 1;
        } else {
            assert false;
        }

        if (waitTime != WAIT_TIME) {
            waitPending = null;
        }
    }
//
//    public synchronized void releaseRead(TransactionId tid) {
//        assert holdingLock.contains(tid);
//        assert readCount > 0;
//        readCount--;
//        if (readCount == 0) {
//            notifyAll();
//        }
//    }
//
//    public synchronized void releaseWrite(TransactionId tid) {
//        assert holdingLock.contains(tid);
//        assert writeCount == 1;
//        writeCount--;
//        notifyAll();
//    }

    public synchronized boolean isHoldingLock(TransactionId tid) {
        return holdingLock.contains(tid);
    }

    public synchronized void releaseLock(TransactionId tid) {
        if (holdingLock.contains(tid)) {
            holdingLock.remove(tid);
            if (readCount > 0) {
                readCount--;
                if (readCount == 0||readCount == 1) {
                    notifyAll();
                }
            }
            if (writeCount == 1) {
                writeCount = 0;
                notifyAll();
            }
        } else {
            assert false;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("readCount:").append(readCount).append(" writeCount：").append(writeCount).append(" tids :").append(holdingLock);
        return builder.toString();
    }
}


class LockManagerOld {
    private Map<PageId, ReentrantReadWriteLock> hash;
    private Map<TransactionId, Set<ReentrantReadWriteLock.ReadLock>> readLockTable;
    private Map<TransactionId, Set<ReentrantReadWriteLock.WriteLock>> writeLockTable;

    public LockManagerOld() {
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
