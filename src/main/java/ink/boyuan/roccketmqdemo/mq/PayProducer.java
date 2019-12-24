package ink.boyuan.roccketmqdemo.mq;

import ink.boyuan.roccketmqdemo.config.JmsConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * @author wyy
 * @version 1.0
 * @date 2019/12/23 15:18
 * @description
 **/
@Component
public class PayProducer {
    /**
     * 生产者的组名
     */
    private String producerGroup="PayProducer";



    private DefaultMQProducer producer;

    /**
     * 构造方法初始化
     * 注：构造方法先于变量初始化所以全局变量赋值无法注入
     * 类似 @Value是无法注入到构造方法参数里面的
     */
    public PayProducer(){
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        start();
    }

    public DefaultMQProducer getProducer(){
        return this.producer;
    }

    /**
     * 一般应用在上下文，使用上下文监听
     */
    public void shutdowm(){
        this.producer.shutdown();
    }

    /**
     * Producer对象在使用之前必须要调用start初始化，初始化一次即可
     * 注意：切记不可以在每次发送消息时，都调用start方法
     */
    public void start(){
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public SendResult send(String topic,String tag,String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(topic,tag,text.getBytes());
        SendResult sendResult = producer.send(message);
        shutdowm();
        return sendResult;
    }

    public SendResult sendMessage(Message message) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = producer.send(message);
        shutdowm();
        return sendResult;
    }

}
