// The parseq package provides a simple interface for processing a stream in parallel,
// with configurable level of parallelism, while still outputting a sequential stream
// that respects the order of input.
package parseq

import (
	"sync"
)

type ParSeq struct {
	// Input is the channel the client code should send to.
	Input chan interface{}

	// Output is the channel the client code should recieve from. Output order respects
	// input order. Output is usually casted to a more useful type.
	Output chan interface{}

	parallelism int
	work        []chan interface{}
	outs        []chan interface{}
	process     []ProcessFunc
}
type ProcessFunc func(interface{}) interface{}
type processFuncGenerator func(int) (ProcessFunc, error)

// New returns a new ParSeq. Processing doesn't begin until the Start method is called.
// ParSeq is concurrency-safe; multiple ParSeqs can run in parallel.
// `parallelism` determines how many goroutines read from the Input channel, and each
// of the goroutines uses the `process` function to process the inputs.
func New(parallelism int, processGenerator processFuncGenerator) (*ParSeq, error) {
	var err error
	process := make([]ProcessFunc, parallelism)
	for i := 0; i < parallelism; i++ {
		process[i], err = processGenerator(i)
		if err != nil {
			return nil, err
		}
	}

	work := make([]chan interface{}, parallelism)
	outs := make([]chan interface{}, parallelism)
	for i := 0; i < parallelism; i++ {
		work[i] = make(chan interface{}, parallelism)
		outs[i] = make(chan interface{}, parallelism)
	}

	return &ParSeq{
		Input:  make(chan interface{}, parallelism),
		Output: make(chan interface{}, parallelism),

		parallelism: parallelism,
		work:        work,
		outs:        outs,
		process:     process,
	}, nil
}

// Start begins consuming the Input channel and producing to the Output channel.
// It starts n+2 goroutines, n being the level of parallelism, so Close should be
// called to exit the goroutines after processing has finished.
func (p *ParSeq) Start() {
	go p.readRequests()
	go p.orderResults()

	var wg sync.WaitGroup
	for i := 0; i < p.parallelism; i++ {
		wg.Add(1)
		go p.processRequests(&wg, p.work[i], p.outs[i], p.process[i])
	}

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		for _, o := range p.outs {
			close(o)
		}
	}(&wg)
}

// Close waits for all queued messages to process, and stops the ParSeq.
// This ParSeq cannot be used after calling Close(). You must not send
// to the Input channel after calling Close().
func (p *ParSeq) Close() {
	close(p.Input)
}

func (p *ParSeq) readRequests() {
	i := 0
	for r := range p.Input {
		p.work[i%p.parallelism] <- r
		i++
		if i >= p.parallelism {
			i = 0
		}
	}
	for _, w := range p.work {
		close(w)
	}
}

func (p *ParSeq) processRequests(wg *sync.WaitGroup, in chan interface{}, out chan interface{}, processFunc ProcessFunc) {
	defer wg.Done()

	for r := range in {
		out <- processFunc(r)
	}
}

func (p *ParSeq) orderResults() {
	for {
		for i := 0; i < p.parallelism; i++ {
			val, ok := <-p.outs[i]
			if !ok {
				close(p.Output)
				return
			}
			p.Output <- val
		}
	}
}
