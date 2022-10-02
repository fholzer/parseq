package parseq

import (
	"sync"
	"testing"
	"time"

	"math/rand"
)

func TestOutperformsSequential(t *testing.T) {
	p, err := New(5, processAfter(50*time.Millisecond))
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
	elapsed := time.Now().Sub(start)

	if elapsed > 150*time.Millisecond { // 150ms for
		t.Error("test took too long; parallel strategy is ineffective!")
	}
	t.Log("min(elapsed) was 90ms; took", elapsed, "against sequential time of 250ms")

	p.Close()
}

func TestOrderedOutput(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	p, err := New(5, processAfterRandom(r))
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

	if a.(int) != 666 ||
		b.(int) != 667 ||
		c.(int) != 668 ||
		d.(int) != 669 ||
		e.(int) != 670 {
		t.Error("output came out out of order: ", a, b, c, d, e)
	}
}

func processAfter(d time.Duration) processFuncGenerator {
	return processGenerator(func(v interface{}) interface{} {
		time.Sleep(d)
		return v
	})
}

func processAfterRandom(r *rand.Rand) processFuncGenerator {
	var mu sync.Mutex
	return processGenerator(func(v interface{}) interface{} {
		mu.Lock()
		rnd := r.Intn(41)
		mu.Unlock()
		time.Sleep(time.Duration(rnd+10) * time.Millisecond) //sleep between 10ms and 50ms
		return v
	})
}

func processGenerator(f func(interface{}) interface{}) processFuncGenerator {
	return func(i int) (ProcessFunc, error) {
		return f, nil
	}
}
