package ro.fortech.demo.sparkconnection;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkConnectionSample {
	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static final int NUM_SAMPLE = 10000;

	public static void main(String[] args) throws UnknownHostException {
		
		String host = InetAddress.getLocalHost().getHostAddress();
		System.out.println(host);
		
	    conf = (new SparkConf()).setAppName("PI spark")
	      .setMaster("spark://192.168.203.20:7077")
	      .set("spark.network.timeout", "800")
	      .set("spark.driver.bindAddress", "172.17.0.2")
	      .set("spark.driver.host", "192.168.204.48")
	      .set("spark.scheduler.mode", "FAIR")
	      .set("spark.executor.memory", "1g");
    
		sc = new JavaSparkContext(conf);

		List<Integer> l = new ArrayList<Integer>(10000);
		for (int i = 0; i < 10000; i++) {
			l.add(Integer.valueOf(i));
		}
		long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				double x = Math.random();
				double y = Math.random();
				return x * x + y * y < 1;
			}

		}).count();

		System.out.println("Pi is roughly " + (4.0D * count / 10000.0D));

		sc.stop();
	}
}
