package ink.boyuan.roccketmqdemo.mq;

import ink.boyuan.roccketmqdemo.config.JmsConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author wyy
 * @version 1.0
 * @date 2019/12/23 15:19
 * @description
 **/
@Component
public class Consumer {

    /**
     * 消费者的组名
     */
    private String consumerGroup="PayConsumer";

    private DefaultMQPushConsumer consumer;

    public  Consumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.subscribe(JmsConfig.TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                Message msg = msgs.get(0);
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                String topic = msg.getTopic();
                String body = new String(msg.getBody(), "utf-8");
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

//            consumer.registerMessageListener( new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                try {
//                    Message msg = msgs.get(0);
//                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
//
//                    String topic = msg.getTopic();
//                    String body = new String(msg.getBody(), "utf-8");
//                    String tags = msg.getTags();
//                    String keys = msg.getKeys();
//                    System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
//
//                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                } catch (UnsupportedEncodingException e) {
//
//                    e.printStackTrace();
//                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                }
//            }
//        });
        consumer.start();
        System.out.println("consumer start ...");
    }
}
