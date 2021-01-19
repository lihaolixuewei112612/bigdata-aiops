package eventone;

import com.google.common.eventbus.EventBus;

/**
 * Copyright @ 2018 lihao.com
 * All right reserved.
 *
 * @author Li Hao
 * @since 2018/9/30  19:30
 */
public class TestEventBus {
    public static void main(String[] args) throws Exception {
        TestEventBus te = new TestEventBus();
        te.testReceiveEvent();
    }

    public void testReceiveEvent() throws Exception {

        EventBus eventBus = new EventBus("kafka");
        EventListener listener = new EventListener();

        eventBus.register(listener);

        eventBus.post(new TestEvent(200));
        eventBus.post(new TestEvent(300));
        eventBus.post(new TestEvent(400));

        System.out.println("LastMessage:" + listener.getLastMessage());
    }
}