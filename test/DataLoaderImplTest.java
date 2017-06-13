import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.esgyn.dataloader.impl.SingleDataLoaderImpl;
import com.esgyn.tools.DBUtil;

public class DataLoaderImplTest {
	@Test
	public void testSingleLoader() {
		Properties prop = DBUtil.readProperties();
		SingleDataLoaderImpl loader1=new SingleDataLoaderImpl(prop);
		Thread th1 = new Thread(loader1);
		th1.start();
		while (th1.isAlive()) {
			try {
				th1.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	@Test
	public void testWithFixedPool() {
		ExecutorService service =Executors.newFixedThreadPool(1);
		SingleDataLoaderImpl loader=new SingleDataLoaderImpl();
		service.submit(loader);
		while (!service.isTerminated()) {
		}
		service.shutdown();
		System.out.println("***************************************");
	}
	@Test
	public void testCustomThreadPool() {
		long start = System.currentTimeMillis();
		//shared queue between producer and customer
		BlockingQueue<StringBuilder> queue=new ArrayBlockingQueue<StringBuilder>(10000);
		//maximum size for the query count or table count
		BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(2);
		ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 10, 5000, TimeUnit.MILLISECONDS,
				blockingQueue);

		executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				System.out.println("Waiting for a second !!");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 executor.submit(r);
			}
		});
		// Let start all core threads initially
		executor.prestartAllCoreThreads();
		/*executor.submit(new SourceImpl(queue));
		for (int i = 0; i < 4; i++) {
			executor.submit(new TargetImpl(queue));
		}*/
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
		java.text.DecimalFormat   df   =new   java.text.DecimalFormat("#"); 
		long elapsedTimeMillis = System.currentTimeMillis()-start;
		String elapsedTimeMin = df.format(elapsedTimeMillis/(60*1000F));
		float elapsedTimeSec = (elapsedTimeMillis%(60*1000F))/1000F;
		System.out.println("the total time cost: " + elapsedTimeMin + " minutes; " + elapsedTimeSec + " seconds");
	}
}
