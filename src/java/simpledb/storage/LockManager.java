package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

import java.util.*;

public class LockManager {
    
    private Map<PageId, Lock> lockTable;
    private Map<TransactionId, WaitInfo> waitingFor;

    public LockManager(){
        lockTable = new HashMap<>();
        waitingFor = new HashMap<>();
    }

    private class Lock{
        Set<TransactionId> sharedLock;
        TransactionId exclusiveLock;

        Lock(){
            sharedLock = new HashSet<>();
            exclusiveLock = null;
        }
    }

    private class WaitInfo{
        PageId pid;
        Permissions perm;

        WaitInfo(PageId pid, Permissions perm){
            this.pid = pid;
            this.perm = perm;
        }
    }

        private Set<TransactionId> getBlockers(TransactionId tid, PageId pid, Permissions perm){
       Set<TransactionId> blockers = new HashSet<>();
       Lock lock = lockTable.get(pid);
       
       // no blcokers 
       if (lock == null){
            return blockers;
        }

        // Blocked by exclusive lock
        if (lock.exclusiveLock != null && !lock.exclusiveLock.equals(tid)){
            blockers.add(lock.exclusiveLock);
        }
        
        // Blocked by shared locks if requesting exclusive lock
        if (perm == Permissions.READ_WRITE){
            for (TransactionId sharedTid: lock.sharedLock){
                if (!sharedTid.equals(tid)){
                    blockers.add(sharedTid); // set ignores dupes
                }
            }
        }

        return blockers;
    }

    private boolean detectDeadlock(TransactionId tid, PageId pid, Permissions perm){
        
        Set<TransactionId> visited = new HashSet<>();
        Deque<TransactionId> stack = new ArrayDeque<>();
        
        stack.addAll(getBlockers(tid, pid, perm));
        
        while (!stack.isEmpty()){
            TransactionId currentTid = stack.pop();
            
            // If we reach the original transaction, a deadlock is detected
            if (currentTid.equals(tid)){
                return true;
            }
            

            if (!visited.contains(currentTid)){
                visited.add(currentTid);

                WaitInfo waitInfo = waitingFor.get(currentTid);

                if (waitInfo != null){
                    for (TransactionId blocker: getBlockers(currentTid, waitInfo.pid, waitInfo.perm)){
                        stack.push(blocker);
                    }

                }
            }
        }
        return false;
    }
    
    /*Acquires the lock for the page based on given permissions*/
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException{
        Lock lock = lockTable.get(pid);

        if (lock == null){
            lock = new Lock();
            lockTable.put(pid, lock);
        }

        try {
            /*Gain sharedLock if not in exclusiveLock*/
            if (perm == Permissions.READ_ONLY){
                while(lock.exclusiveLock != null && !lock.exclusiveLock.equals(tid)){

                    waitingFor.put(tid, new WaitInfo(pid, perm));
                    if (detectDeadlock(tid, pid, perm)){
                        throw new TransactionAbortedException();
                    }

                    try{
                        wait();
                    } catch (InterruptedException e){
                        throw new TransactionAbortedException();
                    }
                }

                lock.sharedLock.add(tid);

            }

            /*Gain exclusiveLock if not in exclusiveLock or upgrading from sharedLock to exclusiveLock*/
            if (perm == Permissions.READ_WRITE){
                while(lock.exclusiveLock != null && !lock.exclusiveLock.equals(tid) || (!lock.sharedLock.isEmpty() && !(lock.sharedLock.size() == 1 && lock.sharedLock.contains(tid)))){
                    waitingFor.put(tid, new WaitInfo(pid, perm));
                    if (detectDeadlock(tid, pid, perm)){
                        throw new TransactionAbortedException();
                    }
                    try{
                        wait();
                    } catch (InterruptedException e){
                        throw new TransactionAbortedException();
                    }
                }

                lock.sharedLock.remove(tid);
                lock.exclusiveLock = tid;
            }        
        } finally {
            waitingFor.remove(tid); // for cleaning up the waitingFor map after acquiring the lock
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

    /*Checks if a page is currently holding onto any kind of lock*/
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        Lock lock = lockTable.get(pid);

        if (lock == null){
            return false;
        }

        return (lock.sharedLock.contains(tid) || (lock.exclusiveLock != null && lock.exclusiveLock.equals(tid)));
    }

    /*Iterates through all locks and releases them*/
    public synchronized void releaseAllLocks(TransactionId tid){
        waitingFor.remove(tid); // Clean up waitingFor map for the transaction
        for (PageId pid: lockTable.keySet()){
            releaseLock(tid, pid);
        }
    }
}
