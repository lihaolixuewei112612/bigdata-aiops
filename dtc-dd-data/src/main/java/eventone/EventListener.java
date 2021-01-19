package eventone;
import com.google.common.eventbus.Subscribe;

/**
 * Copyright @ 2018 lihao.com
 * All right reserved.
 *
 * @author Li Hao
 * @since 2018/9/30  19:30
 */
public class EventListener {
    public int lastMessage = 0;

    @Subscribe
    public void lihao(TestEvent event) {
        lastMessage = event.getMessage();
        System.out.println("Message:" + lastMessage);
    }

    public int getLastMessage() {
        return lastMessage;
    }
}