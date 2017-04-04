package plugin

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin/rpc"
	
)

type StreamProxy struct {
	pluginProxy

	plugin 			 StreamCollector

	MaxMetricsBuffer         int64
	MaxCollectDuration	 time.Duration
	lastTime	 	 time.Time

	buffer 			 *streamBuffer
}

func (p *StreamProxy) StreamMetrics(stream rpc.StreamCollector_StreamMetricsServer) error {
	// Error channel where we will forward plugin errors to snap where it
	// can report/handle them.
	errChan := make(chan string)
	// Metrics into the plugin from snap.
	inChan := make(chan []Metric)
	// Metrics out of the plugin into snap.
	outChan := make(chan []Metric)
	err := p.plugin.StreamMetrics(inChan, outChan, errChan)

	p.lastTime = time.Now()

	if err != nil {
		return err
	}
	fmt.Println("Debug Iza1, ze stara kolejnosc!!!")
	go p.metricSend(outChan, stream)
	//todo iza- tu nie musi być stream proxy interface
	go p.errorSend(errChan, stream)
	p.streamRecv(inChan, stream)
	return nil
}

type streamBuffer struct {
	metrics 	 	[]*rpc.Metric
	overflow		[]*rpc.Metric
	capacity        	 int64
	lifetime		 time.Duration
	lastTimeFlushed	 	 time.Time
}

func (b *streamBuffer) put(metric *rpc.Metric) {
	if b.capacity > 0 && int64(len(b.metrics)) > b.capacity {
		// buffer capacity is exceeded, so append it as overflowed metrics
		b.overflow = append(b.metrics, metric)
	}
	// buffer capacity is no exceeded, so append it as regular buffered metrics
	b.metrics = append(b.metrics, metric)
}

func (b *streamBuffer) get() []*rpc.Metric {
	return b.metrics
}

func (b *streamBuffer) isReadyToFlush() bool {

	if b.capacity > 0 && time.Since(b.lastTimeFlushed) < b.lifetime {
		// still buffer data
		fmt.Println("Debug Iza1, module: isFullBuffer, still BUFFERING metrics")
		return false
	}
	return true
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

func newStreamBuffer(maxMetricsBuffer int64, maxCollectDuration time.Duration) *streamBuffer {
	return &streamBuffer{
		metrics: []*rpc.Metric{},
		overflow: []*rpc.Metric{},
		capacity: maxMetricsBuffer,
		lifetime: maxCollectDuration,
		lastTimeFlushed: time.Now(),
	}
}

func (b *streamBuffer) flush(s rpc.StreamCollector_StreamMetricsServer) error {
	// prepare a reply and send it
	reply := &rpc.CollectReply{
			Metrics_Reply: &rpc.MetricsReply{Metrics: b.metrics},
	}
	if err := s.Send(reply); err != nil {
		return err
	}
	// set the last time when buffer has been flushed
	b.lastTimeFlushed = time.Now()

	// drain buffered metrics
	b.metrics = []*rpc.Metric{}
	//  overflowed metrics from previous batch, append them to buffer
	//todo iza - it's dangerous
	for _, metric := range b.overflow {
		b.put(metric)
	}

	return nil
}

func (p *StreamProxy) metricSend(ch chan []Metric, s rpc.StreamCollector_StreamMetricsServer) {

	fmt.Println("Debug Iza1, module: metricSend")
	fmt.Println("Debug Iza1, module: metricSend; available maxMetricBuffer=%v", p.MaxMetricsBuffer)
	fmt.Println("Debug Iza1, module: metricSend; available maxMaxCollectDuration=%v", p.MaxCollectDuration)
	fmt.Println("Debug Iza1, module: metricSend, lastTime=%v", p.lastTime)
	fmt.Println("Debug Iza1, module: metricSend, time.Since(p.lastTime)=%v", time.Since(p.lastTime))
	p.buffer = newStreamBuffer(p.MaxMetricsBuffer, p.MaxCollectDuration)

	for r := range ch {
		fmt.Println("Debug Iza1, module: metricSend, len(R)=%v", len(r))
		//mts := []*rpc.Metric{}
		for _, mt := range r {
			metric, err := toProtoMetric(mt)
			if err != nil {
				fmt.Println(err.Error())
			}
			// do buffering
			p.buffer.put(metric)
		}

		if p.isFullBuffer() {

			p.buffer.flush(s)
			// prepare a reply and send it
			reply := &rpc.CollectReply{
				Metrics_Reply: &rpc.MetricsReply{Metrics: p.buffer.get()},
			}
			if err := s.Send(reply); err != nil {
				fmt.Println(err.Error())
			}

			// reset
			p.buffer.lastTimeFlushed = time.Now()
		}
	}

}

func (p *StreamProxy) isFullBuffer()  bool {
	fmt.Println("Debug Iza1, module: isFullBuffer, check duration=%v", time.Since(p.lastTime))

	if p.MaxMetricsBuffer > 0 && int64(len(p.buffer.metrics)) >= p.MaxMetricsBuffer {
		// still buffer data
		fmt.Println("Debug Iza1, module: maxMetricsBufferIsExceeded, reached metrics buffer limit")
		return true
	}

	if p.MaxCollectDuration > 0 && time.Since(p.lastTime) >= p.MaxCollectDuration {
		// still buffer data
		fmt.Println("Debug Iza1, module: isFullBuffer, reached collect duration")
		return true
	}

	// still buffer data
	fmt.Println("Debug Iza1, module: isFullBuffer, still BUFFERING metrics")
	return false
}

//func (p *StreamProxy) maxCollectDurationIsExceeded()  bool {
//	if p.MaxCollectDuration > 0 && time.Since(p.lastTime) < p.MaxCollectDuration {
//		// still buffer data
//		fmt.Println("Debug Iza1, module: maxCollectDurationIsExceeded, still BUFFERING metrics")
//		return false
//	}
//
//	return true
//}
//
//func (p *StreamProxy) maxMetricsBufferIsExceeded()  bool {
//	if p.MaxMetricsBuffer > 0 && len(p.buffer.metrics) < p.MaxMetricsBuffer {
//		// still buffer data
//		fmt.Println("Debug Iza1, module: maxMetricsBufferIsExceeded, still BUFFERING metrics")
//		return false
//	}
//
//	return true
//}




//todo iza - to sie wykonuje tylko raz na poczatku
func (p *StreamProxy) streamRecv(ch chan []Metric, s rpc.StreamCollector_StreamMetricsServer) {

	fmt.Println("Debug Iza1, module: streamRecv")
	for {
		s, err := s.Recv()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if s != nil {
			//todo iza
			if s.MaxMetricsBuffer != 0 {
				fmt.Println("Debug Iza1, module: streamRecv, set max metrics buffer")
				p.setMaxMetricsBuffer(s.MaxMetricsBuffer)

				fmt.Println("!!!Debug Iza1, stream_collector_proxy.go streamRecv: setting max buffer to: %v", p.MaxMetricsBuffer)
				fmt.Println("Debug Iza, module: streamRecv, set max metrics buffer")

			}
			if s.MaxCollectDuration != 0 {
				fmt.Println("Debug Iza1, module: streamRecv, set max collect duration")
				p.setMaxCollectDuration(time.Duration(s.MaxCollectDuration))

				fmt.Println("!!!Debug Iza1, stream_collector_proxy.go streamRecv: setting max collect duration: %v", p.MaxCollectDuration)

				//plugin.setMaxCollectDuration(time.Duration(s.MaxCollectDuration))
			}
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

func (p *StreamProxy) setMaxCollectDuration(i time.Duration) {
	p.MaxCollectDuration = i
}
func (p *StreamProxy) setMaxMetricsBuffer(i int64) {
	p.MaxMetricsBuffer = i
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
