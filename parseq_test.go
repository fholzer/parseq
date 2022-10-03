package parseq

import (
	"testing"
	"time"

	"math/rand"
)

func TestOutperformsSequential(t *testing.T) {
	p, err := NewWithProcessor[int, int](5, &ConstantDelayProcessor{
		delay: 50 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}

	go p.Start()
	go func() {
		p.Input <- 666
		time.Sleep(10 * time.Millisecond)
		p.Input <- 667
		time.Sleep(10 * time.Millisecond)
		p.Input <- 668
		time.Sleep(10 * time.Millisecond)
		p.Input <- 669
		time.Sleep(10 * time.Millisecond)
		p.Input <- 670
	}()

	start := time.Now()
	<-p.Output // min(elapsed)=50ms
	<-p.Output // min(elapsed)=60ms
	<-p.Output // min(elapsed)=70ms
	<-p.Output // min(elapsed)=80ms
	<-p.Output // min(elapsed)=90ms
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond { // 150ms for
		t.Error("test took too long; parallel strategy is ineffective!")
	}
	t.Log("min(elapsed) was 90ms; took", elapsed, "against sequential time of 250ms")

	p.Close()
}

func TestOrderedOutput(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	p, err := NewWithProcessor[int, int](5, &RandomDelayProcessor{
		r: r,
	})
	if err != nil {
		panic(err)
	}

	go p.Start()
	go func() {
		p.Input <- 666
		time.Sleep(10 * time.Millisecond)
		p.Input <- 667
		time.Sleep(10 * time.Millisecond)
		p.Input <- 668
		time.Sleep(10 * time.Millisecond)
		p.Input <- 669
		time.Sleep(10 * time.Millisecond)
		p.Input <- 670
		p.Close()
	}()

	a := <-p.Output
	b := <-p.Output
	c := <-p.Output
	d := <-p.Output
	time.Sleep(10 * time.Millisecond)
	e := <-p.Output

	if a != 666 ||
		b != 667 ||
		c != 668 ||
		d != 669 ||
		e != 670 {
		t.Error("output came out out of order: ", a, b, c, d, e)
	}
}

type ConstantDelayProcessor struct {
	delay time.Duration
}

func (p *ConstantDelayProcessor) Process(i int) int {
	time.Sleep(time.Duration(p.delay))
	return i
}

type RandomDelayProcessor struct {
	r *rand.Rand
}

func (p *RandomDelayProcessor) Process(i int) int {
	time.Sleep(time.Duration(p.r.Intn(41)+10) * time.Millisecond) //sleep between
	return i
}
