import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.esgyn.dataloader.impl.DataLoaderImpl;

public class DataLoaderImplTest {
	@Test
	public void testMain(){
		ExecutorService executor=Executors.newFixedThreadPool(5);
		DataLoaderImpl loader = new DataLoaderImpl();
		executor.execute(loader);
		executor.shutdown();
		while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
	}
}
