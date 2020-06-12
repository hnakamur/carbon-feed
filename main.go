package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type Metric struct {
	itemPrefix string
	timestamp  int64
	value      float64
}

type Sender struct {
	svID    string
	dest    string
	timeout time.Duration
	rnd     *rand.Rand
}

func main() {
	dest := flag.String("dest", "127.0.0.1:2003", "destination")
	timeout := flag.Duration("timeout", 10*time.Second, "timeout")
	svCount := flag.Int("sv-count", 30, "source server count")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := run(*dest, *timeout, *svCount); err != nil {
		log.Fatal(err)
	}
}

func run(dest string, timeout time.Duration, svCount int) error {
	metricC := make(chan Metric, svCount)
	var g errgroup.Group
	for i := 0; i < svCount; i++ {
		svID := fmt.Sprintf("sv%02d", i)
		g.Go(func() error {
			s := newSender(svID, dest, timeout)
			return s.run(metricC)
		})
	}
	go func() {
		for {
			total := float64(0)
			var t0 int64
			for i := 0; i < svCount; i++ {
				m := <-metricC
				if i == 0 {
					t0 = m.timestamp
				} else if m.timestamp != t0 {
					log.Printf("timestamp should match, t0=%d, t[%d]=%d", t0, i, m.timestamp)
				}
				total += m.value
			}
			log.Printf("sent data timestamp=%s, total=%s",
				time.Unix(t0, 0).Format("2006-01-02T15:04:05Z"),
				strconv.FormatFloat(total, 'f', -1, 64))
		}
	}()
	return g.Wait()
}

func newSender(svID, dest string, timeout time.Duration) *Sender {
	return &Sender{
		svID:    svID,
		dest:    dest,
		timeout: timeout,
		rnd:     rand.New(rand.NewSource(newRandSeed())),
	}
}

func (s *Sender) run(metricC chan<- Metric) error {
	for {
		now := time.Now()
		targetTime := now.Truncate(time.Minute)
		durTillNextMin := targetTime.Add(time.Minute).Sub(now)
		time.Sleep(durTillNextMin)

		m := genRandMetric(targetTime, s.rnd)
		if err := s.send(m); err != nil {
			return err
		}
		metricC <- *m
	}
	return nil
}

func (s *Sender) send(m *Metric) error {
	d := net.Dialer{Timeout: s.timeout}
	conn, err := d.Dial("tcp", s.dest)
	if err != nil {
		return err
	}
	defer conn.Close()

	var b strings.Builder
	fmt.Fprintf(&b, "%s%s %s %d\n",
		m.itemPrefix,
		s.svID,
		strconv.FormatFloat(m.value, 'f', -1, 64),
		m.timestamp)
	data := []byte(b.String())
	if _, err = conn.Write(data); err != nil {
		return err
	}

	return nil
}

func genRandMetric(t time.Time, rnd *rand.Rand) *Metric {
	return &Metric{
		itemPrefix: "test.foo.",
		value:      float64(rnd.Intn(100)),
		timestamp:  t.Unix(),
	}
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}
