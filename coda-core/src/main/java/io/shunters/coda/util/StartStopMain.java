package io.shunters.coda.util;

import io.shunters.coda.server.CodaServer;


public class StartStopMain {
	
	private static Thread t;
	
	private static final String DEFAULT_CONF = "any-conf";
	
	public static void main(String[] args) throws Exception{

		// TODO:
		// configuration file path to be passed.
		// shutdown hook to be added.
		
		String methodName = args[0];
		System.out.println("methodName: [" + methodName + "]");		
		
		if(methodName.equals("start"))
		{  		
			String conf = args[1];
			if(conf == null)
			{
				conf = DEFAULT_CONF;
			}
			
			start(conf);
		}
		else if(methodName.equals("stop"))
		{
			stop();
		}
		else
		{
			System.exit(1);
		}
		
	}
	

	public static void start(String conf) throws Exception
	{
		CodaServer server = new CodaServer(9911, 50);

		t = new Thread(server);
		t.start();
	}  	

	public static void stop()
	{
		t = null;
	}
}

