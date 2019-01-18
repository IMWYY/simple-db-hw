package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class main {

	public static void main(String[] args) throws InterruptedException {
		TestList testList = new TestList();
		Thread t = new Thread(() -> {
			try {
				testList.visit(1, 0);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		t.start();
		testList.visit(1, 1);
	}

}

class TestList {
	ConcurrentHashMap<Integer, Object> map;

	TestList() {
		this.map = new ConcurrentHashMap<>();
		this.map.put(0, new Object());
		this.map.put(1, new Object());
		this.map.put(2, new Object());
	}

	void visit(int n, int s) throws InterruptedException {
		System.out.println("wait here: " + s);
		synchronized (this.map.get(n)) {
			System.out.println("hhhhhhhhhh:" + s);
			Thread.sleep(3000);
		}
	}

}