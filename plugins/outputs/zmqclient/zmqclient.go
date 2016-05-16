package zmqclient

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"os"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

type ZMQClient struct {
	closers     []io.Closer
	Zmqendpoint string
	Zmqsvc      string

	ctx    *zmq.Context
	poller *zmq.Poller
	connxn *zmq.Socket

	serializer serializers.Serializer
}

func NewZMQClient() *ZMQClient {

	tctx, err := zmq.NewContext()
	if err != nil {
		return nil
	}
	tconnxn, err := tctx.NewSocket(zmq.DEALER)
	if err != nil {
		return nil
	}

	return (&ZMQClient{
		ctx:    tctx,
		connxn: tconnxn})
}

var sampleConfig = `
  ## zmqclient 
  zmqendpoint = ["tcp://127.0.0.1:9999"]
  zmqsvc = "telegraf"

  ## Data format to output.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
`

func (z *ZMQClient) SetSerializer(serializer serializers.Serializer) {
	z.serializer = serializer
}

func (z *ZMQClient) Send(message []byte) (n int, err error) {

	n, err = z.connxn.Send("", zmq.SNDMORE)
	if err != nil {
		return n, err
	}
	z.connxn.Send(z.Zmqsvc, zmq.SNDMORE)
	if err != nil {
		return n, err
	}
	z.connxn.Send("", zmq.SNDMORE)
	if err != nil {
		return n, err
	}
	return z.connxn.SendBytes(message, zmq.DONTWAIT)

}

func (z *ZMQClient) Close() error {
	return z.connxn.Close()
}

func (z *ZMQClient) Connect() error {
	z.closers = append(z.closers, z)

	ident, err := os.Hostname()
	if err != nil {
		ident = "unknown"
	}
	z.connxn.SetIdentity(ident)
	return z.connxn.Connect(z.Zmqendpoint) // address of zmqBroker

}

func (z *ZMQClient) SampleConfig() string {
	return sampleConfig
}

func (z *ZMQClient) Description() string {
	return "Send telegraf metrics to file(s)"
}

func (z *ZMQClient) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	for _, metric := range metrics {
		values, err := z.serializer.Serialize(metric)
		if err != nil {
			return err
		}

		for _, value := range values {
			_, err = z.Send([]byte(value + "\n"))
			if err != nil {
				return fmt.Errorf("FAILED to write message: %s, %s", value, err)
			}
		}
	}
	return nil
}

func init() {
	outputs.Add("zmqclient", func() telegraf.Output {
		return NewZMQClient()
	})
}
