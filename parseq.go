// The parseq package provides a simple interface for processing a stream in parallel,
// with configurable level of parallelism, while still outputting a sequential stream
// that respects the order of input.
package parseq

import (
	"fmt"
	"sync"
)

type ParSeq struct {
	// Input is the channel the client code should send to.
	Input chan interface{}

	// Output is the channel the client code should recieve from. Output order respects
	// input order. Output is usually casted to a more useful type.
	Output chan interface{}

	parallelism int
	work        []chan *workPackage
	outChan     chan *workPackage
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
		process[i], err = processGenerator(int(i))
		if err != nil {
			return nil, err
		}
	}

	work := make([]chan *workPackage, parallelism)  // must have size equal to parallelism
	outChan := make(chan *workPackage, parallelism) // must have buffer size equal to parallelism
	for i := 0; i < parallelism; i++ {
		work[i] = make(chan *workPackage) // must be unbuffered
	}

	return &ParSeq{
		Input:  make(chan interface{}, parallelism),
		Output: make(chan interface{}, parallelism),

		parallelism: parallelism,
		work:        work,
		outChan:     outChan,
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
		go p.processRequests(&wg, p.work[i], p.outChan, p.process[i])
	}

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(p.outChan)
	}(&wg)
}

// Close waits for all queued messages to process, and stops the ParSeq.
// This ParSeq cannot be used after calling Close(). You must not send
// to the Input channel after calling Close().
func (p *ParSeq) Close() {
	close(p.Input)
}

type workPackage struct {
	data  interface{}
	order int64
}

func (p *ParSeq) readRequests() {
	i := 0
	par := p.parallelism
	order := int64(0)
	for r := range p.Input {
		p.work[i%par] <- &workPackage{
			data:  r,
			order: order,
		}
		i++
		order++
		if i >= par {
			i = 0
		}
	}
	for _, w := range p.work {
		close(w)
	}
}

func (p *ParSeq) processRequests(wg *sync.WaitGroup, in chan *workPackage, out chan *workPackage, processFunc ProcessFunc) {
	defer wg.Done()

	for r := range in {
		r.data = processFunc(r.data)
		if r.data == nil {
			fmt.Println("processFun returned nil value")
		}
		out <- r
	}
}

const debug bool = false

func (p *ParSeq) orderResults() {
	parallelism := p.parallelism
	if debug {
		fmt.Printf("[%p] parallelism: %d\n", p, parallelism)
	}
	sliceLimit := 2 * parallelism
	sliceLimit64 := int64(sliceLimit)
	outSlice := make([]*workPackage, sliceLimit) // must have size equal to parallelism
	sent := int64(0)
	got := int64(0)
	nextOffset := 0
	for val := range p.outChan {
		// store
		slot := val.order % sliceLimit64
		if debug {
			fmt.Printf("[%p] setting result #%d in slot %d\n", p, val.order, slot)
		}
		outSlice[slot] = val
		got++

		// retry:
		w := outSlice[nextOffset]
		if w != nil && w.order == sent {
			if debug {
				fmt.Printf("[%p] getting result #%d from slot %d\n", p, w.order, nextOffset)
			}
			p.Output <- w.data
			sent++
			nextOffset++
			if nextOffset >= sliceLimit {
				if debug {
					fmt.Printf("[%p] resetting `nextOffset` to 0\n", p)
				}
				nextOffset = 0
			}
		}
	}

	if debug {
		fmt.Printf("[%p] got %d, sent %d\n", p, got, sent)
	}

	// send remaining values
	for i := sent; i < got; i++ {
		if debug {
			fmt.Printf("[%p] sending remaining %d\n", p, got-sent)
		}

		w := outSlice[nextOffset]
		if w != nil && w.order == sent {
			if debug {
				fmt.Printf("[%p] getting result #%d from slot %d\n", p, w.order, nextOffset)
			}
			p.Output <- w.data
			sent++
			nextOffset++
			if nextOffset >= sliceLimit {
				if debug {
					fmt.Printf("[%p] resetting `nextOffset` to 0\n", p)
				}
				nextOffset = 0
			}
		} else {
			panic(fmt.Sprintf("[%p] trying to send remaining %d values, but found nil or wrong order for result %d in slot %d: %+v\nslice: %+v", p, got-sent, i, nextOffset, w, outSlice))
		}
	}

	close(p.Output)
}
