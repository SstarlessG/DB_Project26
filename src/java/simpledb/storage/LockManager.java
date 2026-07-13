package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

import java.util.*;

public class LockManager {
    
    private Map<PageId, Lock> lockTable;

    public LockManager(){
        lockTable = new HashMap<>();
    }

    private class Lock{
        Set<TransactionId> sharedLock;
        TransactionId exclusiveLock;

        Lock(){
            sharedLock = new HashSet<>();
            exclusiveLock = null;
        }
    }

    /*Acquires the lock for the page based on given permissions*/
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException{
        Lock lock = lockTable.get(pid);

        if (lock == null){
            lock = new Lock();
            lockTable.put(pid, lock);
        }

        /*Gain sharedLock if not in exclusiveLock*/
        if (perm == Permissions.READ_ONLY){
            while(lock.exclusiveLock != null && !lock.exclusiveLock.equals(tid)){
                try{
                    wait();
                } catch (InterruptedException e){
                    throw new TransactionAbortedException();
                }
            }

            lock.sharedLock.add(tid);

        }

        /*Gain exclusiveLock if not in exclusiveLock or sharedLock*/
        if (perm == Permissions.READ_WRITE){
            while(lock.exclusiveLock != null && !lock.exclusiveLock.equals(tid) || (!lock.sharedLock.isEmpty() && !(lock.sharedLock.size() == 1 && lock.sharedLock.contains(tid)))){
                try{
                    wait();
                } catch (InterruptedException e){
                    throw new TransactionAbortedException();
                }
            }

            lock.sharedLock.remove(tid);
            lock.exclusiveLock = tid;
        }
    }

    /*Releases both sharedLock and exclusiveLock of the page*/
    public synchronized void releaseLock(TransactionId tid, PageId pid){
        Lock lock = lockTable.get(pid);

        if (lock == null){
            return;
        }

        lock.sharedLock.remove(tid);

        if (lock.exclusiveLock != null && lock.exclusiveLock.equals(tid)){
            lock.exclusiveLock = null;
        }

        notifyAll();
    }

    /*Checks if a page is currently holding onto a lock*/
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        Lock lock = lockTable.get(pid);

        if (lock == null){
            return false;
        }

        return (lock.sharedLock.contains(tid) || (lock.exclusiveLock != null && lock.exclusiveLock.equals(tid)));
    }

    /*Iterates through all locks and releases them*/
    public synchronized void releaseAllLocks(TransactionId tid){
        for (PageId pid: lockTable.keySet()){
            releaseLock(tid, pid);
        }
    }
}
