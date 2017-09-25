package io.shunters.coda.discovery;

import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.server.CodaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mykidong on 2017-09-14.
 */
public class ConsulSessionHolder implements Runnable {

    private static Logger log = LoggerFactory.getLogger(ConsulSessionHolder.class);

    private ServiceDiscovery serviceDiscovery;

    private String hostName;

    private int port;

    private int ttl;

    private String session;

    private boolean shutdown = false;

    private String sessionName;

    private String nodeDescription;

    private String key;

    private int brokerId;


    public ConsulSessionHolder(String sessionName, String key, int brokerId, String hostName, int port, int ttl) {
        this.sessionName = sessionName;
        this.brokerId = brokerId;
        this.hostName = hostName;
        this.port = port;
        this.ttl = ttl;
        this.key = key;

        this.serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();

        session = serviceDiscovery.createSession(this.sessionName, this.hostName, ttl + "s", 10);
        log.info("session: {}: ", session);

        nodeDescription = ServiceDiscovery.ServiceNode.describe(this.brokerId,this.hostName, this.port);

        boolean lockAcquired = serviceDiscovery.acquireLock(key, nodeDescription, session);

        log.info("node desc: {}, lock acquired: {}", nodeDescription, String.valueOf(lockAcquired));

        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {

        while (!shutdown)
        {
            serviceDiscovery.renewSession(session);
            boolean lockAcquired = serviceDiscovery.acquireLock(this.key, nodeDescription, session);

            try {
                Thread.sleep(ttl / 2 * 1000);
            } catch (InterruptedException e)
            {
                e.printStackTrace();

                continue;
            }
        }

    }

    public void shutdown()
    {
        shutdown = true;

        this.serviceDiscovery.destroySession(this.session);
    }

}
