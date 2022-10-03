// The parseq package provides a simple interface for processing a stream in parallel,
// with configurable level of parallelism, while still outputting a sequential stream
// that respects the order of input.
package parseq

import (
	"fmt"
	"sync"
)

type ParSeq[InType any, OutType any] struct {
	// Input is the channel the client code should send to.
	Input chan InType

	// Output is the channel the client code should recieve from. Output order respects
	// input order. Output is usually casted to a more useful type.
	Output chan OutType

	parallelism int
	work        []chan InType
	outs        []chan OutType
	process     []Processor[InType, OutType]
}
type Processor[InType any, OutType any] interface {
	Process(InType) OutType
}
type ProcessorGenerator[InType any, OutType any] interface {
	GenerateProcessor(int) (Processor[InType, OutType], error)
}

// New returns a new ParSeq. Processing doesn't begin until the Start method is called.
// ParSeq is concurrency-safe; multiple ParSeqs can run in parallel.
// `parallelism` determines how many goroutines read from the Input channel, and each
// of the goroutines uses the `process` function to process the inputs.
func NewWithProcessorSlice[InType any, OutType any](parallelism int, processors []Processor[InType, OutType]) (*ParSeq[InType, OutType], error) {
	if len(processors) != parallelism {
		return nil, fmt.Errorf("length of processors slice (%d) must equal to parallelism (%d)", len(processors), parallelism)
	}

	work := make([]chan InType, parallelism)
	outs := make([]chan OutType, parallelism)
	for i := 0; i < parallelism; i++ {
		work[i] = make(chan InType, parallelism)
		outs[i] = make(chan OutType, parallelism)
	}

	return &ParSeq[InType, OutType]{
		Input:  make(chan InType, parallelism),
		Output: make(chan OutType, parallelism),

		parallelism: parallelism,
		work:        work,
		outs:        outs,
		process:     processors,
	}, nil
}

func NewWithProcessorGenerator[InType any, OutType any](parallelism int, processorGenerator ProcessorGenerator[InType, OutType]) (*ParSeq[InType, OutType], error) {
	var err error
	processors := make([]Processor[InType, OutType], parallelism)
	for i := 0; i < parallelism; i++ {
		processors[i], err = processorGenerator.GenerateProcessor(i)
		if err != nil {
			return nil, err
		}
	}
	return NewWithProcessorSlice[InType, OutType](parallelism, processors)
}

func NewWithProcessor[InType any, OutType any](parallelism int, processor Processor[InType, OutType]) (*ParSeq[InType, OutType], error) {
	processors := make([]Processor[InType, OutType], parallelism)
	for i := 0; i < parallelism; i++ {
		processors[i] = processor
	}
	return NewWithProcessorSlice[InType, OutType](parallelism, processors)
}

// Start begins consuming the Input channel and producing to the Output channel.
// It starts n+2 goroutines, n being the level of parallelism, so Close should be
// called to exit the goroutines after processing has finished.
func (p *ParSeq[InType, OutType]) Start() {
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
func (p *ParSeq[InType, OutType]) Close() {
	close(p.Input)
}

func (p *ParSeq[InType, OutType]) readRequests() {
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

func (p *ParSeq[InType, OutType]) processRequests(wg *sync.WaitGroup, in chan InType, out chan OutType, processFunc Processor[InType, OutType]) {
	defer wg.Done()

	for r := range in {
		out <- processFunc.Process(r)
	}
}

func (p *ParSeq[InType, OutType]) orderResults() {
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
