package org.confr.storage;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ZkValueDispatcherTest {
    private static TestingServer zkTestingServer;
    private static ZkValueDispatcher dispatcher;
    @BeforeClass
    public static void initClass() throws Exception {
        zkTestingServer = new TestingServer(true);
        dispatcher = new ZkValueDispatcher(zkTestingServer.getConnectString(), "confr");
        dispatcher.start();
    }

    @AfterClass
    public static void destoryClass() throws IOException {
        dispatcher.shutdown();
        zkTestingServer.stop();
        zkTestingServer.close();
    }

    private String key;

    @Before
    public void initContext() {
        key = UUID.randomUUID().toString();
    }

    @Test
    public void testRegister() throws Exception {
        String path = ZkValueDispatcher.zkPathForKey(key);
        dispatcher.getClient().newNamespaceAwareEnsurePath(path).ensure(dispatcher.getClient().getZookeeperClient());

        int valueChangeCount = 10;
        List<String> values = IntStream.range(0, valueChangeCount).mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());
        CountDownLatch exitLatch = new CountDownLatch(valueChangeCount);
        List<String> listenedValues = new ArrayList<>(valueChangeCount);
        String initValue = dispatcher.register(key, new ValueChangeListener() {
            @Override public void onChanged(ValueChangedEvent event) {
                listenedValues.add(event.getData());
                exitLatch.countDown();
            }
        });
        assertNotNull(initValue);
        assertTrue("initValue should be empty", initValue.isEmpty());

        for (String value : values) {
            setData(key, value);
            // sleep for a while is necessary, if too quick, zk cannot push all the change.
            Thread.sleep(50);
        }
        // set value
        exitLatch.await(5, TimeUnit.SECONDS);
        assertTrue(
                "countdown should be equal to valueChangedCount",
                exitLatch.getCount() == 0
        );
        assertEquals("listened values should be equal to expected values", values, listenedValues);
    }


    @Test
    public void testDeregister() throws Exception {
        ValueChangeListener listener = new ValueChangeListener() {
            @Override public void onChanged(ValueChangedEvent event) {
                // do nothing
            }
        };

        Runnable registerTask = new Runnable() {
            @Override public void run() {
                try {
                    dispatcher.register(key, listener);
                } catch (Exception e) {
                }
            }
        };

        Set<Thread> registerThreads = IntStream.range(0, 10).mapToObj(i -> new Thread(registerTask))
                .collect(Collectors.toSet());
        registerThreads.forEach(Thread::start);
        for (Thread registerThread : registerThreads) {
            registerThread.join();
        }

        assertEquals(1, dispatcher.getWatchers().size());
        assertEquals(1, dispatcher.getWatchers().get(key).getListenerContainer().size());

        dispatcher.deregister(key, listener);
        assertEquals(0, dispatcher.getWatchers().size());
    }

    private void setData(String key, String value) throws Exception {
        String path = ZkValueDispatcher.zkPathForKey(key);

        dispatcher.getClient().setData().forPath(path, value.getBytes());
    }
}
