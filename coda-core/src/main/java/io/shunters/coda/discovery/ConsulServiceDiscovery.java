package io.shunters.coda.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.SessionClient;
import com.ecwid.consul.v1.session.SessionConsulClient;
import com.ecwid.consul.v1.session.model.NewSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Leader Election:
 * 1. create session: curl  -X PUT -d '{"Name": "coda"}' http://localhost:8500/v1/session/create
 * 2. acquire lock: curl -X PUT -d <body> http://localhost:8500/v1/kv/<key>?acquire=<session>
 * 3. discover leader: curl  http://localhost:8500/v1/kv/<key>
 */
public class ConsulServiceDiscovery implements ServiceDiscovery {

    private static Logger log = LoggerFactory.getLogger(ConsulServiceDiscovery.class);

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8500;

    private ConsulClient client;

    private SessionClient sessionClient;

    private String agentHost;

    private int agentPort;

    private static final Object lock = new Object();

    private static ServiceDiscovery serviceDiscovery;

    public static ServiceDiscovery getConsulServiceDiscovery() {
        return ConsulServiceDiscovery.singleton(DEFAULT_HOST, DEFAULT_PORT);
    }

    public static ServiceDiscovery singleton(String agentHost, int agentPort) {
        if (serviceDiscovery == null) {
            synchronized (lock) {
                if (serviceDiscovery == null) {
                    serviceDiscovery = new ConsulServiceDiscovery(agentHost, agentPort);
                }
            }
        }
        return serviceDiscovery;
    }


    private ConsulServiceDiscovery(String agentHost, int agentPort) {
        client = new ConsulClient(agentHost, agentPort);
        sessionClient = new SessionConsulClient(agentHost, agentPort);

        log.info("client info " + client.toString());
    }

    /**
     * @param serviceName
     * @param id
     * @param tags
     * @param address
     * @param port
     * @param script
     * @param tcp         "localhost:9911"
     * @param interval    "10s"
     * @param timeout     "1s"
     */
    @Override
    public void createService(String serviceName, String id, List<String> tags, String address, int port, String script, String tcp, String interval, String timeout) {
        // register new service with associated health check
        NewService newService = new NewService();
        newService.setName(serviceName);
        newService.setId(id);
        newService.setAddress(address);
        newService.setPort(port);
        if (tags != null) newService.setTags(tags);

        NewService.Check serviceCheck = new NewService.Check();
        if (script != null) serviceCheck.setScript(script);
        if (tcp != null) serviceCheck.setTcp(tcp);
        serviceCheck.setInterval(interval);
        if (timeout != null) serviceCheck.setTimeout(timeout);
        newService.setCheck(serviceCheck);

        client.agentServiceRegister(newService);
    }


    @Override
    public List<HostPort> getHealthServices(String serviceName) {

        List<HostPort> list = new ArrayList<>();

        Response<List<HealthService>> healthServiceResponse = client.getHealthServices(serviceName, true, QueryParams.DEFAULT);

        List<HealthService> healthServices = healthServiceResponse.getValue();
        if (healthServices == null) {
            return null;
        }

        for (HealthService healthService : healthServices) {
            HealthService.Service service = healthService.getService();

            String address = healthService.getNode().getAddress();

            int port = service.getPort();

            list.add(new HostPort(address, port));
        }

        return list;
    }

    @Override
    public Map<String, String> getKVValues(String keyPath) {

        Response<List<GetValue>> valueResponse = client.getKVValues(keyPath);

        List<GetValue> getValues = valueResponse.getValue();

        if (getValues == null) {
            return null;
        }

        Map<String, String> map = new HashMap<>();

        for (GetValue v : getValues) {
            if (v == null || v.getValue() == null) {
                continue;
            }

            String key = v.getKey();

            String value = v.getDecodedValue();

            map.put(key, value);
        }

        return map;
    }

    @Override
    public void setKVValue(String key, String value) {

        client.setKVValue(key, value);
    }

    @Override
    public String createSession(String name, String node) {
        NewSession newSession = new NewSession();
        newSession.setName(name);
        newSession.setNode(node);

        Response<String> response = this.sessionClient.sessionCreate(newSession, QueryParams.DEFAULT);

        return response.getValue();
    }

    @Override
    public void destroySession(String session) {
        this.sessionClient.sessionDestroy(session, QueryParams.DEFAULT);
    }

    @Override
    public boolean acquireLock(String key, String value, String session) {

        PutParams putParams = new PutParams();
        putParams.setAcquireSession(session);

        Response<Boolean> response = client.setKVValue(key, value, putParams);

        return response.getValue();
    }
}