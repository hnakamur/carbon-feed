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
	var g errgroup.Group
	for i := 0; i < svCount; i++ {
		svID := fmt.Sprintf("sv%02d", i)
		g.Go(func() error {
			s := newSender(svID, dest, timeout)
			return s.run()
		})
	}
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

func (s *Sender) run() error {
	for {
		now := time.Now()
		durTillNextMin := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
		time.Sleep(durTillNextMin)

		metrics := genRandMetrics(s.rnd)
		if err := s.send(metrics); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sender) send(metrics []Metric) error {
	d := net.Dialer{Timeout: s.timeout}
	conn, err := d.Dial("tcp", s.dest)
	if err != nil {
		return err
	}
	defer conn.Close()

	var b strings.Builder
	for _, m := range metrics {
		fmt.Fprintf(&b, "%s%s %s %d\n",
			m.itemPrefix,
			s.svID,
			strconv.FormatFloat(m.value, 'f', -1, 64),
			m.timestamp)
	}
	data := []byte(b.String())
	if _, err = conn.Write(data); err != nil {
		return err
	}
	log.Printf("sent data=%s\n", string(data))

	return nil
}

func genRandMetrics(rnd *rand.Rand) []Metric {
	return []Metric{
		{
			itemPrefix: "test.foo.",
			value:      float64(rnd.Intn(100)),
			timestamp:  time.Now().Unix(),
		},
	}
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}
