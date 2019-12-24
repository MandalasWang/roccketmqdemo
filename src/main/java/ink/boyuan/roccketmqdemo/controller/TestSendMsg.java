package ink.boyuan.roccketmqdemo.controller;

import ink.boyuan.roccketmqdemo.config.JmsConfig;
import ink.boyuan.roccketmqdemo.mq.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wyy
 * @version 1.0
 * @date 2019/12/23 15:21
 * @description
 **/
@RestController
@RequestMapping("/")
public class TestSendMsg {

    @Autowired
    private PayProducer payProducer;


    @RequestMapping("send")
    public String send(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC,"taga", ("hello xdclass rocketmq = "+text).getBytes() );
        //SendResult sendResult = payProducer.sendMessage(message);
        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println(sendResult);
        return "success";
    }





}
