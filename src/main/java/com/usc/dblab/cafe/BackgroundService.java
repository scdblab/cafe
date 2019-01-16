package com.usc.dblab.cafe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle background worker threads.
 * @author hieun
 *
 */
public class BackgroundService {
	public static BackgroundWorker[] workers = null;
	public static ExecutorService service;
	
	private static final AtomicBoolean isInit = new AtomicBoolean(false);
	private static final Semaphore lock = new Semaphore(1);
	
	/**
	 * Initialize background workers.
	 * @param cafe
	 * @param cachePoolName
	 * @param numBackgroundWorkers
	 * @param pass 
	 * @param user 
	 * @param url 
	 */
	public static void init(NgCache cafe, String cachePoolName, int numBackgroundWorkers, 
	        String url, String user, String pass, int minId, int maxId) {
		try {
			lock.acquire();
			
			if (!isInit.get()) {
				workers = new BackgroundWorker[numBackgroundWorkers];
				for (int i = 0; i < numBackgroundWorkers; i++) {
				    if (minId + i <= maxId) {
				        workers[i] = new BackgroundWorker(minId + i, cafe, cachePoolName, url, user, pass);
				    }
				}
				
				for (int i = 0; i < numBackgroundWorkers; i++) {
				    if (workers[i] != null) {
				        workers[i].start();
				    }
				}
				
				if (service == null) {
				    service = Executors.newFixedThreadPool(Config.replicas * 5);
				}
				
				isInit.set(true);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			lock.release();
		}
	}
	
	/**
	 * Clean-up background workers.
	 */
	public static void clean() {
		try {
			lock.acquire();
			if (isInit.get()) {
				if (workers != null) {
					for (int i = 0; i < workers.length; i++) {
					    if (workers[i] != null) {
					        workers[i].setRunning(false);
					    }
					}
					workers = null;
				}
				if (service != null) {
				    service.shutdown();
				    service = null;
				}
				isInit.set(false);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			lock.release();
		}
	}

    public static ExecutorService getService() {
        return service;
    }
}