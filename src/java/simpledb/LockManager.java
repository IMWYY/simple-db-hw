package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction Lock Manager. Offer Page level Lock and unlock control.
 */
public class LockManager {

	// 记录每个page的锁状态
	private ConcurrentHashMap<PageId, List<LockNode>> lockState;

	// 用于实现page级别的锁同步
	private ConcurrentHashMap<PageId, Object> synchronizeControl;

	// 当前有依赖的tid集合 用于死锁检测
	private ConcurrentHashMap<TransactionId, Set<TransactionId>> dependencyOnTids;

	// 当前正在等待获取锁的tid集合
	private Set<TransactionId> waitingTransactions;

	public LockManager() {
		this.lockState = new ConcurrentHashMap<>();
		this.synchronizeControl = new ConcurrentHashMap<>();
		this.dependencyOnTids = new ConcurrentHashMap<>();
		this.waitingTransactions = new HashSet<>();
	}

	public boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions) {
		Debug.log("[LockManager#acquireLock] start acquire tid=%d, tableId=%d, pageNo=%d, perm=%s",
				tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions.toString());

		this.synchronizeControl.putIfAbsent(pid, new Object());
		this.lockState.putIfAbsent(pid, new LinkedList<>());

		// 这里添加一个waitingTids 是为解决以下这种场景：
		// t1 acquires p0.read; t2 acquires p0.read;
		// t1 attempts to upgrade to p0.write; t2 attempts to upgrade to p0.write
		// 因为获取的是同一个page的锁，所以当t1正在while循环获取的时候，是没办法感知t2正被拦在synchronized外面等待获取同一个page的锁
		// 所以记录一个waiting list.
		this.waitingTransactions.add(tid);

		int loopCount = 0;

		synchronized (this.synchronizeControl.get(pid)) {
			this.waitingTransactions.remove(tid);

			while (true) {
				if (lockState.get(pid).size() == 0) {
					lockState.get(pid).add(new LockNode(tid, permissions, pid));
					this.dependencyOnTids.remove(tid);
					return true;
				}

				// 加S锁 如果没有X锁就可以加S锁
				// 注意这里如果是同一个事务之前有X锁然后现在变成S锁是允许的 需要特殊处理
				if (permissions == Permissions.READ_ONLY) {
					boolean hasXLock = false;
					for (LockNode n : lockState.get(pid)) {
						if (n.isXLock() && !n.belongTo(tid)) {
							hasXLock = true;
							this.dependencyOnTids.putIfAbsent(tid, new HashSet<>());
							this.dependencyOnTids.get(tid).add(n.tid);
						}
					}
					if (!hasXLock) {
						this.lockState.get(pid).add(new LockNode(tid, permissions, pid));
						this.dependencyOnTids.remove(tid);
						return true;
					} else {
						if (hasDeadLockDependency(this.dependencyOnTids, this.waitingTransactions, tid, tid,
								new HashSet<>())) {
							this.dependencyOnTids.remove(tid);

							Debug.log(Debug.LEVEL_DEBUG, "hasDeadLock:  tid=%d, tableId=%d, pageNo=%d, perm=%s",
									tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions);

							return false;
						}
					}

					// 加X锁 如果有其他事务同时用到这个page则不能升级
				} else {
					boolean canUpdateLock = true;
					for (LockNode n : lockState.get(pid)) {
						if (!n.belongTo(tid)) {
							canUpdateLock = false;
							this.dependencyOnTids.putIfAbsent(tid, new HashSet<>());
							this.dependencyOnTids.get(tid).add(n.tid);
						}
					}
					if (canUpdateLock) {
						for (LockNode n : lockState.get(pid)) {
							n.upgradeToXLock();
						}
						this.dependencyOnTids.remove(tid);
						return true;
					} else {
						if (hasDeadLockDependency(this.dependencyOnTids, this.waitingTransactions, tid, tid,
								new HashSet<>())) {
							this.dependencyOnTids.remove(tid);

							Debug.log(Debug.LEVEL_DEBUG, "hasDeadLock:  tid=%d, tableId=%d, pageNo=%d, perm=%s",
									tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions);

							return false;
						}
					}
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				Debug.log(Debug.LEVEL_DEBUG, "[LockManager#acquireLock] tid=%d, tableId=%d, pageNo=%d, perm=%s",
						tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions);

				// timeout deadlock detection
				if (++loopCount > 10) {
					return false;
				}
			}
		}
	}

	/**
	 * 有死锁的条件是：有事务循环依赖（包括正在等待获取锁的事务）
	 */
	private boolean hasDeadLockDependency(ConcurrentHashMap<TransactionId, Set<TransactionId>> dependency,
			Set<TransactionId> waitingTids, TransactionId curTid, TransactionId loopTid, Set<TransactionId> visited) {
		if (visited.contains(loopTid))
			return false;
		visited.add(loopTid);
		for (TransactionId t : dependency.getOrDefault(loopTid, new HashSet<>())) {
			if (t.equals(curTid) || waitingTids.contains(t) ||
					hasDeadLockDependency(dependency, waitingTids, curTid, t, visited)) {
				return true;
			}
		}
		return false;
	}

	//	/**
	//	 * 仅仅利用timeout来实现的死锁检测
	//	 */
	//	public boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions) {
	//		Debug.log("[LockManager#acquireLock] start acquire tid=%d, tableId=%d, pageNo=%d, perm=%s",
	//				tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions.toString());
	//
	//		this.synchronizeControl.putIfAbsent(pid, new Object());
	//		this.lockState.putIfAbsent(pid, new LinkedList<>());
	//
	//		int loopCount = 0;
	//
	//		synchronized (this.synchronizeControl.get(pid)) {
	//			while (true) {
	//				if (lockState.get(pid).size() == 0) {
	//					lockState.get(pid).add(new LockNode(tid, permissions, pid));
	//					return true;
	//				}
	//
	//				// 加S锁 如果没有X锁就可以加S锁
	//				// 注意这里如果是同一个事务之前有X锁然后现在变成S锁是允许的 需要特殊处理
	//				if (permissions == Permissions.READ_ONLY) {
	//					boolean hasXLock = false;
	//					for (LockNode n : lockState.get(pid)) {
	//						if (n.isXLock()) {
	//							if (!n.belongTo(tid)) {
	//								hasXLock = true;
	//                              break;
	//							}
	//						}
	//					}
	//					if (!hasXLock) {
	//						lockState.get(pid).add(new LockNode(tid, permissions, pid));
	//						return true;
	//					}
	//					// 加X锁 如果有其他事务同时用到这个page则不能升级
	//				} else {
	//					boolean canUpdateLock = true;
	//					for (LockNode n : lockState.get(pid)) {
	//						if (!n.belongTo(tid)) {
	//							canUpdateLock = false;
	//							break;
	//						}
	//					}
	//					if (canUpdateLock) {
	//						for (LockNode n : lockState.get(pid)) {
	//							n.upgradeToXLock();
	//						}
	//						return true;
	//					}
	//				}
	//
	//				try {
	//					Thread.sleep(100);
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}
	//
	//				Debug.log(Debug.LEVEL_DEBUG, "[LockManager#acquireLock] tid=%d, tableId=%d, pageNo=%d, perm=%s",
	//						tid.getId(), pid.getTableId(), pid.getPageNumber(), permissions);
	//
	//				// timeout deadlock detection
	//				if (++loopCount > 5) {
	//					return false;
	//				}
	//			}
	//		}
	//	}

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

	/**
	 * 释放用于pageID并发控制的object 防止synchronizeControl一直增长
	 *
	 * @param pid
	 */
	public void releasePageIdLock(PageId pid) {
		this.synchronizeControl.remove(pid);
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


