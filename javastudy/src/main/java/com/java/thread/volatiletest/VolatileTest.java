package com.java.thread.volatiletest;

public class VolatileTest {

	public static volatile int race = 0;
	
	/**
	 * 同步锁 20000
	 */
	public synchronized static void synincrease(){
		race ++;
	}
	
	/**
	 * 不安全的累加
	 */
	public  static void increase(){
		race ++;
	}
	
	private static final int THREAD_COUNTS = 20;
	
	public static void main(String[] args) {
		Thread[] threads = new Thread[THREAD_COUNTS];
		for(int i=0;i<THREAD_COUNTS;i++){
			threads[i] = new Thread(new Runnable(){
				
				public void run(){
					for(int i = 0;i<10000;i++){
						increase();
					}
				}
				
				
			});
			threads[i].start();
		}
		
		while(Thread.activeCount() > 1){
			Thread.yield();
		}

		System.out.println(race);
	}

}
