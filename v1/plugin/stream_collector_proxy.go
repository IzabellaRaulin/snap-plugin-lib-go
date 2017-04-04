package plugin

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin/rpc"
)

type StreamProxy struct {
	pluginProxy
	plugin StreamCollector
	buffer *streamBuffer
}

func (p *StreamProxy) StreamMetrics(stream rpc.StreamCollector_StreamMetricsServer) error {
	// Error channel where we will forward plugin errors to snap where it
	// can report/handle them.
	errChan := make(chan string)
	// Metrics into the plugin from snap.
	inChan := make(chan []Metric)
	// Metrics out of the plugin into snap.
	outChan := make(chan []Metric)
	p.buffer = newStreamBuffer()
	err := p.plugin.StreamMetrics(inChan, outChan, errChan)
	if err != nil {
		return err
	}
	go p.metricSend(outChan, stream)
	go p.errorSend(errChan, stream)
	p.streamRecv(inChan, stream)
	return nil
}

type streamBuffer struct {
	metrics            []*rpc.Metric
	maxMetricsBuffer   int64
	maxCollectDuration time.Duration
	lastTimeFlushed    time.Time
}

func (p *StreamProxy) errorSend(ch chan string, s rpc.StreamCollector_StreamMetricsServer) {
	for r := range ch {
		reply := &rpc.CollectReply{
			Error: &rpc.ErrReply{Error: r},
		}
		if err := s.Send(reply); err != nil {
			fmt.Println(err.Error())
		}

	}
}

func newStreamBuffer() *streamBuffer {
	return &streamBuffer{
		metrics:            []*rpc.Metric{},
		maxMetricsBuffer:   0,
		maxCollectDuration: time.Duration(0),
		lastTimeFlushed:    time.Now(),
	}
}

func (b *streamBuffer) flush(s rpc.StreamCollector_StreamMetricsServer) error {
	// prepare a reply and send it
	reply := &rpc.CollectReply{
		Metrics_Reply: &rpc.MetricsReply{Metrics: b.get()},
	}
	err := s.Send(reply)

	// set the last time when buffer has been flushed
	b.lastTimeFlushed = time.Now()
	// drain buffered metrics
	b.metrics = []*rpc.Metric{}

	return err
}

func (b *streamBuffer) put(metric *rpc.Metric) {
	b.metrics = append(b.metrics, metric)
}

func (b *streamBuffer) get() []*rpc.Metric {
	return b.metrics
}

func (p *StreamProxy) metricSend(ch chan []Metric, stream rpc.StreamCollector_StreamMetricsServer) {
	fmt.Println("!!!!!!!Debug Iza1, module: metricSend!!!!!!!!")
	for r := range ch {
		for _, mt := range r {
			metric, err := toProtoMetric(mt)
			if err != nil {
				fmt.Println(err.Error())
			}
			p.buffer.put(metric)
		}

		if p.buffer.isReadyToBeFlush() {
			// prepare a reply and send it
			if err := p.buffer.flush(stream); err != nil {
				fmt.Println(err.Error())
			}
		} else {
			//to do iza - log about it
		}

	}
}

func (b *streamBuffer) isReadyToBeFlush() bool {
	// check if maxMetricsBuffer is exceeded
	if b.maxMetricsBuffer > 0 && int64(len(b.metrics)) >= b.maxMetricsBuffer {
		fmt.Println("Debug Iza1, module: isReadyToBeFlush, reached metrics buffer limit")
		return true
	}
	// check if maxCollectDuration is exceeded
	if b.maxCollectDuration > 0 && time.Since(b.lastTimeFlushed) >= b.maxCollectDuration {
		fmt.Println("Debug Iza1, module: isReadyToBeFlush, reached collect duration")
		return true
	}

	// still buffer data
	fmt.Println("Debug Iza, module: isReadyToBeFlush, still BUFFERING metrics")
	return false
}

func (p *StreamProxy) streamRecv(ch chan []Metric, s rpc.StreamCollector_StreamMetricsServer) {
	fmt.Println("Debug Iza1, module: streamRecv")
	for {
		s, err := s.Recv()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if s != nil {
			p.buffer.setMaxMetricsBuffer(s.MaxMetricsBuffer)
			p.buffer.setMaxCollectDuration(time.Duration(s.MaxCollectDuration))

			if s.Metrics_Arg != nil {
				metrics := []Metric{}
				for _, mt := range s.Metrics_Arg.Metrics {
					metric := fromProtoMetric(mt)
					metrics = append(metrics, metric)
				}
				ch <- metrics
			}
		}
	}
}

func (b *streamBuffer) setMaxCollectDuration(i time.Duration) {
	b.maxCollectDuration = i
}

func (b *streamBuffer) setMaxMetricsBuffer(i int64) {
	b.maxMetricsBuffer = i
}

func (p *StreamProxy) SetConfig(context.Context, *rpc.ConfigMap) (*rpc.ErrReply, error) {
	return nil, nil
}

func (p *StreamProxy) GetMetricTypes(ctx context.Context, arg *rpc.GetMetricTypesArg) (*rpc.MetricsReply, error) {
	cfg := fromProtoConfig(arg.Config)

	r, err := p.plugin.GetMetricTypes(cfg)
	if err != nil {
		return nil, err
	}
	metrics := []*rpc.Metric{}
	for _, mt := range r {
		// We can ignore this error since we are not returning data from
		// GetMetricTypes.
		metric, _ := toProtoMetric(mt)
		metrics = append(metrics, metric)
	}
	reply := &rpc.MetricsReply{
		Metrics: metrics,
	}
	return reply, nil
}
