package ro.fortech.demo.sparkconnection;

import java.io.File;
import java.io.IOException;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

/**
 * 192.168.203.20
 *
 */
public class RemoteLauncher {
	
	private final static String JAR_NAME = "sparkconnection-0.0.1-SNAPSHOT.jar";
	private final static String SPARK_HOME = System.getenv("SPARK_HOME");
	private final static String JAVA_HOME = System.getenv("JAVA_HOME");
	private final static String APP_MAIN_CLASS = "ro.fortech.demo.sparkconnection.SparkConnectionSample";
	private final static String MASTER = "spark://192.168.203.20:7077";
	
	static {
		new File("D://output//").mkdirs();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {

		final String appResource = SPARK_HOME + "\\examples\\" + JAR_NAME;
		
		final File sparkBin = new File(SPARK_HOME+ "//bin");
		final File redirectOutFile = new File("D://output//redirectOut.txt");		
		final File redirectError = new File("D://output//redirectError.txt");
		
		SparkLauncher sparkLauncher = new SparkLauncher()
				.setAppName("PiExample")
				.setAppResource(appResource)
				.setSparkHome(SPARK_HOME)
				.setJavaHome(JAVA_HOME)
				.setMaster(MASTER)
				.setMainClass(APP_MAIN_CLASS);
		
		// Enables verbose reporting for SparkSubmit.
		sparkLauncher.setVerbose(true);
		
		// "client" mode, launches the driver outside of the cluster
		sparkLauncher.setDeployMode("client");
		// Set the driver host (this machine)
		sparkLauncher.setConf("spark.driver.host", "192.168.204.48");
		
		// Sets a Spark property. Expects key starting with spark. prefix.
		sparkLauncher.setConf(SparkLauncher.DRIVER_MEMORY, "1g");
		sparkLauncher.setConf(SparkLauncher.EXECUTOR_MEMORY, "1g");
		
		// Sets the working directory of spark-submit
		sparkLauncher.directory(sparkBin);
		// Redirects output to file
		sparkLauncher.redirectOutput(redirectOutFile);
		// Redirects error output to file.
		sparkLauncher.redirectError(redirectError);
		 
		SparkAppHandle appHandle = sparkLauncher.startApplication();
		
		while (appHandle.getState() == State.UNKNOWN) {
			try {
				System.out.println(appHandle.getState().toString());
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		while (appHandle.getState() == State.RUNNING) {
			System.out.println(appHandle.getState().toString());
			Thread.sleep(2000);
		}
		
		System.out.println("Application exit state = " + appHandle.getState().toString());
	
		
	}
}
