package io.shunters.coda.util;

public class TimeUtils {
	public static void pause(long delay){
		long start = System.nanoTime();
		while(start + delay >= System.nanoTime());
	}
}
