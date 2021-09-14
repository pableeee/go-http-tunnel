package tunnel

import (
	"encoding/json"
	"fmt"

	"github.com/mmatczuk/go-http-tunnel/id"
	"github.com/mmatczuk/go-http-tunnel/log"
	"github.com/pableeee/processor/pkg/queue"
)

const (
	eventSubscribe   = "subscribe"
	eventUnsubscribe = "unsubscribe"
)

type NotificationMiddleware struct {
	next         Registry
	publisher    queue.Publisher
	publishTopic string
	logger       log.Logger
}

func newNotificationMiddleware(r Registry, p queue.Publisher, publishTopic string, l log.Logger) Registry {
	return &NotificationMiddleware{
		next:         r,
		publisher:    p,
		publishTopic: publishTopic,
		logger:       l,
	}
}

func (n *NotificationMiddleware) Clear(identifier id.ID) *RegistryItem {
	defer func() { _ = n.publishMessage(eventUnsubscribe, identifier.String(), "dummy-hosty") }()

	return n.next.Clear(identifier)
}

func (n *NotificationMiddleware) Unsubscribe(identifier id.ID) *RegistryItem {
	return n.next.Unsubscribe(identifier)
}

func (n *NotificationMiddleware) Subscribe(identifier id.ID) {
	n.next.Subscribe(identifier)
}

func (n *NotificationMiddleware) IsSubscribed(identifier id.ID) bool {
	return n.next.IsSubscribed(identifier)
}

func (n *NotificationMiddleware) Set(i *RegistryItem, identifier id.ID) error {
	if err := n.next.Set(i, identifier); err != nil {
		return err
	}

	for _, t := range i.Hosts {
		if err := n.publishMessage(eventSubscribe, identifier.String(), t.Host); err != nil {
			continue
		}
	}

	return nil
}

func (n *NotificationMiddleware) Subscriber(hostPort string) (id.ID, *Auth, bool) {
	return n.next.Subscriber(hostPort)
}

func (n *NotificationMiddleware) publishMessage(event, id, host string) error {
	m := map[string]string{
		"event": event,
		"id":    id,
		"host":  host,
	}
	b, err := json.Marshal(m)
	if err != nil {
		n.logger.Log(
			"level", 1,
			"msg", fmt.Sprintf("unable to marshal prometheus update message: %s", err.Error()),
			"err", err,
		)
		return err
	}

	if err = n.publisher.Publish(n.publishTopic, b); err != nil {
		n.logger.Log(
			"level", 1,
			"msg", fmt.Sprintf("unable to pusblish prometheus update message: %s", err.Error()),
			"err", err,
		)

		return err
	}

	n.logger.Log(
		"level", 1,
		"msg", fmt.Sprintf("prometheus update message pusblished: %s", string(b)),
		"err", err,
	)

	return nil
}
