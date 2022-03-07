package aliyun_rocketmq_go

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/zhangsq-ax/logs"
	"go.uber.org/zap"
)

type ConsumeFrom consumer.ConsumeFromWhere

const (
	ConsumeFromLast  ConsumeFrom = ConsumeFrom(consumer.ConsumeFromLastOffset)
	ConsumeFromFirst ConsumeFrom = ConsumeFrom(consumer.ConsumeFromFirstOffset)
)

type RocketHelperOptions struct {
	Endpoint        string      // Aliyun RocketMQ 服务接入点
	InstanceId      string      // Aliyun RocketMQ 服务实例标识
	GroupId         string      // 客户端 Group 标识
	ConsumeFrom     ConsumeFrom // 初次消息消费开始位置
	AccessKeyId     string
	AccessKeySecret string
}

func (opts *RocketHelperOptions) GetCredentials() primitive.Credentials {
	return primitive.Credentials{
		AccessKey: opts.AccessKeyId,
		SecretKey: opts.AccessKeySecret,
	}
}

type RocketHelper struct {
	opts *RocketHelperOptions
}

func NewRocketHelper(opts *RocketHelperOptions) *RocketHelper {
	return &RocketHelper{
		opts: opts,
	}
}

func (rh *RocketHelper) CreatePushConsumer() (rocketmq.PushConsumer, error) {
	opts := rh.opts
	logs.Infow("create-rocketmq-push-consumer",
		zap.Reflect("options", map[string]interface{}{
			"endpoint":   opts.Endpoint,
			"instanceId": opts.InstanceId,
			"groupId":    opts.GroupId,
		}),
	)
	return rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{opts.Endpoint}),
		consumer.WithNamespace(opts.InstanceId),
		consumer.WithInstance(opts.InstanceId),
		consumer.WithGroupName(opts.GroupId),
		consumer.WithCredentials(opts.GetCredentials()),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromWhere(opts.ConsumeFrom)),
		consumer.WithConsumerOrder(true),
	)
}

var pushConsumer rocketmq.PushConsumer

func (rh *RocketHelper) getPushConsumer() (rocketmq.PushConsumer, error) {
	if pushConsumer == nil {
		var err error
		pushConsumer, err = rh.CreatePushConsumer()
		if err != nil {
			return nil, err
		}
	}
	return pushConsumer, nil
}

func (rh *RocketHelper) PushConsumeByConsumer(c rocketmq.PushConsumer, topic string, selector consumer.MessageSelector, onMessage func(*primitive.MessageExt) error) error {
	logs.Infow("subscribe-rocketmq",
		zap.String("topic", topic),
	)
	err := c.Subscribe(topic, selector, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			logs.Debugw("received-rocketmq-message",
				zap.Reflect("summary", map[string]interface{}{
					"messageId":      msg.MsgId,
					"topic":          msg.Topic,
					"queueOffset":    msg.QueueOffset,
					"tags":           msg.GetTags(),
					"keys":           msg.GetKeys(),
					"properties":     msg.GetProperties(),
					"regionId":       msg.GetRegionID(),
					"reconsumeTimes": msg.ReconsumeTimes,
				}),
			)
			err := onMessage(msg)
			if err != nil {
				logs.Warnw("process-message-failed", zap.Error(err))
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}

	return c.Start()
}

func (rh *RocketHelper) PushConsume(topic string, tagFilter string, onMessage func(*primitive.MessageExt) error) error {
	c, err := rh.getPushConsumer()
	if err != nil {
		return err
	}
	return rh.PushConsumeByConsumer(c, topic, consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: tagFilter,
	}, onMessage)
}

func (rh *RocketHelper) NewProducer() (rocketmq.Producer, error) {
	opts := rh.opts
	logs.Infow("create-rocketmq-producer",
		zap.String("endpoint", opts.Endpoint),
		zap.String("instanceId", opts.InstanceId),
		zap.String("groupId", opts.GroupId),
	)
	credentials := opts.GetCredentials()
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{opts.Endpoint}),
		producer.WithNamespace(opts.InstanceId),
		producer.WithInstanceName(opts.InstanceId),
		producer.WithGroupName(opts.GroupId),
		producer.WithCredentials(credentials),
		producer.WithTrace(&primitive.TraceConfig{
			GroupName:    opts.GroupId,
			Access:       primitive.Cloud,
			NamesrvAddrs: []string{opts.Endpoint},
			Credentials:  credentials,
		}),
	)
	if err != nil {
		return nil, err
	}

	err = p.Start()
	return p, err
}

func (rh *RocketHelper) CreatePublicMessage(topic string, body []byte, tag string, keys []string, properties map[string]string) *primitive.Message {
	msg := &primitive.Message{
		Topic: topic,
		Body:  body,
	}
	msg.WithProperties(properties)

	return msg.WithTag(tag).WithKeys(keys)
}

var p rocketmq.Producer

func (rh *RocketHelper) getProducer() (rocketmq.Producer, error) {
	if p == nil {
		var err error
		p, err = rh.NewProducer()
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (rh *RocketHelper) SendMessageByProducer(ctx context.Context, p rocketmq.Producer, msg *primitive.Message) (*primitive.SendResult, error) {
	return p.SendSync(ctx, msg)
}

func (rh *RocketHelper) SendMessage(ctx context.Context, msg *primitive.Message) (*primitive.SendResult, error) {
	p, err := rh.getProducer()
	if err != nil {
		return nil, err
	}
	return rh.SendMessageByProducer(ctx, p, msg)
}
