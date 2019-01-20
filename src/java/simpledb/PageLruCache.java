package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * map container supporting LRU
 */
public class PageLruCache {

	private int capacity;
	private Map<PageId, PageNode> dataMap;
	private PageNode head, tail;  // 伪节点

	public PageLruCache(int capacity) {
		this.capacity = capacity;
		this.dataMap = new HashMap<>();
		this.head = new PageNode(null, null);
		this.tail = new PageNode(null, null);
		head.after = tail;
		tail.before = head;
	}

	public void put(PageId key, Page value) throws DbException {
		if (key == null || value == null) {
			throw new IllegalArgumentException();
		}
		PageNode node = dataMap.get(key);
		if (node != null) {
			node.val = value;
			moveToHead(node);
		} else {
			if (dataMap.size() >= capacity) {
				PageNode n = nextEvictNode();
				if (n != null) {
					this.remove(n.key);
				}
			}
			node = new PageNode(key, value);
			moveToHead(node);
			dataMap.put(key, node);
		}
	}

	public Page get(PageId key) {
		PageNode node = dataMap.get(key);
		if (node != null) {
			moveToHead(node);
			return node.val;
		}
		return null;
	}

	public Page remove(PageId key) {
		PageNode node = this.dataMap.remove(key);
		if (node != null) {
			node.after.before = node.before;
			node.before.after = node.after;
			node.before = null;
			node.after = null;
			return node.val;
		}
		return null;
	}

	public boolean containsKey(PageId key) {
		return this.dataMap.containsKey(key);
	}

	public Iterator<PageNode> iterator() {
		return new PageNodeIterator(head.after);
	}


	/**
	 * 获取下一个被移出的page
	 * 如果这是一个dirty page 则不能移动
	 */
	private PageNode nextEvictNode() throws DbException {
		PageNode s = tail;
		while (s != head) {
			s = s.before;
			if (s.val.isDirty() == null) {
				return s;
			}
		}
		throw new DbException("all page are dirty. No one to evict");
	}

	private void moveToHead(PageNode node) {
		if (node.before == head)
			return;
		if (node.before != null)
			node.before.after = node.after;
		if (node.after != null)
			node.after.before = node.before;
		node.after = head.after;
		node.before = head;
		head.after.before = node;
		head.after = node;
	}

	/**
	 * 双向列表的节点
	 */
	class PageNode {
		PageNode after;
		PageNode before;
		PageId key;
		Page val;

		PageNode(PageId key, Page val) {
			this.val = val;
			this.key = key;
		}
	}

	class PageNodeIterator implements Iterator<PageNode> {

		private PageNode start;

		PageNodeIterator(PageNode start) {
			this.start = start;
		}

		@Override
		public boolean hasNext() {
			return this.start != null && this.start != tail && this.start.after != tail;
		}

		@Override
		public PageNode next() {
			PageNode res = this.start;
			this.start = this.start.after;
			return res;
		}
	}

}

