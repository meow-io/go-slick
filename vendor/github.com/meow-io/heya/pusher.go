package heya

import (
	"github.com/sideshow/apns2"
	apns2_certificate "github.com/sideshow/apns2/certificate"
	"go.uber.org/zap"
)

type Pusher interface {
	DoPush(token string) error
}

type applePusher struct {
	client *apns2.Client
	topic  string
	log    *zap.SugaredLogger
}

func newApplePusher(config *Config, logger *zap.SugaredLogger) (*applePusher, error) {
	cert, err := apns2_certificate.FromP12File(config.APNSCertPath, "")
	if err != nil {
		return nil, err
	}
	client := apns2.NewClient(cert)
	if config.APNSProductionMode {
		client = client.Production()
	} else {
		client = client.Development()
	}

	return &applePusher{
		client: client,
		topic:  config.APNSTopic,
		log:    logger,
	}, nil
}

func (ap *applePusher) DoPush(token string) error {
	body := `{
			"aps": {
				"mutable-content": 1,
				"alert": {
					"title": "New message available"
				}
			}
		}`

	notification := &apns2.Notification{}
	notification.DeviceToken = token
	notification.Topic = ap.topic
	notification.Payload = []byte(body)

	res, err := ap.client.Push(notification)
	if err != nil {
		return err
	}

	ap.log.Debugf("res %#v", res)
	return nil
}
