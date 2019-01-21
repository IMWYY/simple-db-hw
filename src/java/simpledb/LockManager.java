package simpledb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction Lock Manager. Offer Page level Lock and unlock control.
 */
public class LockManager {

	private ConcurrentHashMap<PageId, List<LockNode>> lockState;
	private ConcurrentHashMap<PageId, Object> synchronizeControl;

	public LockManager() {
		this.lockState = new ConcurrentHashMap<>();
		this.synchronizeControl = new ConcurrentHashMap<>();
	}

	public boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions) {
		Debug.log("[LockManager#acquireLock] start acquire tid=%d, tableId=%d, pageNo=%d, perm=%s",
				tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions.toString());

		this.synchronizeControl.putIfAbsent(pid, new Object());
		this.lockState.putIfAbsent(pid, new LinkedList<>());

		int loopCount = 0;

		synchronized (this.synchronizeControl.get(pid)) {
			while (true) {
				if (lockState.get(pid).size() == 0) {
					lockState.get(pid).add(new LockNode(tid, permissions, pid));
					return true;
				}

				// 加S锁 如果没有X锁就可以加S锁
				// 注意这里如果是同一个事务之前有X锁然后现在变成S锁是允许的 需要特殊处理
				if (permissions == Permissions.READ_ONLY) {
					boolean hasXLock = false;
					for (LockNode n : lockState.get(pid)) {
						if (n.isXLock()) {
							if (!n.belongTo(tid)) {
								hasXLock = true;
								break;
							}
						}
					}
					if (!hasXLock) {
						lockState.get(pid).add(new LockNode(tid, permissions, pid));
						return true;
					}
					// 加X锁 如果有其他事务同时用到这个page则不能升级
				} else {
					boolean canUpdateLock = true;
					for (LockNode n : lockState.get(pid)) {
						if (!n.belongTo(tid)) {
							canUpdateLock = false;
							break;
						}
					}
					if (canUpdateLock) {
						for (LockNode n : lockState.get(pid)) {
							n.upgradeToXLock();
						}
						return true;
					}
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				Debug.log(Debug.LEVEL_DEBUG, "[LockManager#acquireLock] tid=%d, tableId=%d, pageNo=%d, perm=%s",
						tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions);

				if (++loopCount > 5) {
					return false;
				}
			}
		}
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		this.synchronizeControl.putIfAbsent(pid, new Object());
		synchronized (this.synchronizeControl.get(pid)) {
			for (LockNode n : this.lockState.get(pid)) {
				if (n.belongTo(tid)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 对于某个事务 释放它在某个page上的锁
	 */
	public boolean releaseLock(TransactionId tid, PageId pid) {
		synchronized (this.synchronizeControl.get(pid)) {
			for (int i = this.lockState.get(pid).size() - 1; i >= 0; i--) {
				if (this.lockState.get(pid).get(i).belongTo(tid)) {
					this.lockState.get(pid).remove(i);
				}
			}
		}
		return true;
	}

	/**
	 * 目前的设计需要遍历这个lockState的map 所以在方法级别加上synchronized
	 */
	public synchronized List<PageId> completeTransaction(TransactionId tid) {
		List<PageId> resList = new ArrayList<>();
		for (Map.Entry<PageId, List<LockNode>> e : this.lockState.entrySet()) {
			for (int i = e.getValue().size() - 1; i >= 0; i--) {
				if (e.getValue().get(i).belongTo(tid)) {
					resList.add(e.getValue().get(i).pid);
					e.getValue().remove(i);
				}
			}
		}
		return resList;
	}

	public synchronized boolean hasDeadLock(TransactionId tid, PageId pid, Permissions perm) {

		return true;
	}

	/**
	 * 获取该事物涉及锁的所有page
	 */
	public synchronized List<PageId> relatedPages(TransactionId tid) {
		List<PageId> resList = new ArrayList<>();
		for (Map.Entry<PageId, List<LockNode>> e : this.lockState.entrySet()) {
			for (LockNode n : e.getValue()) {
				if (n.belongTo(tid)) {
					resList.add(n.pid);
				}
			}
		}
		return resList;
	}

	class LockNode {
		TransactionId tid;
		Permissions permissions;
		PageId pid;

		LockNode(TransactionId tid, Permissions permissions, PageId pid) {
			this.tid = tid;
			this.permissions = permissions;
			this.pid = pid;
		}

		boolean isXLock() {
			return this.permissions == Permissions.READ_WRITE;
		}

		void upgradeToXLock() {
			this.permissions = Permissions.READ_WRITE;
		}

		boolean belongTo(TransactionId tid) {
			return this.tid.equals(tid);
		}
	}
}


