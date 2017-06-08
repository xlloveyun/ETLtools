import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.esgyn.dataloader.impl.DataLoaderImpl;
import com.esgyn.tools.DBUtil;

public class DataLoaderImplTest {
	@Test
	public void testSingleLoader() {
		DataLoaderImpl loader=new DataLoaderImpl();
		Properties prop = DBUtil.readProperties();
		loader.loadData(prop);
		/*Thread th = new Thread(loader);
		th.start();
		while (th.isAlive()) {
			try {
				th.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
	}
	@Test
	public void testWithFixedPool() {
		ExecutorService service =Executors.newFixedThreadPool(1);
		DataLoaderImpl loader=new DataLoaderImpl();
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
