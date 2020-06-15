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
	item      string
	timestamp int64
	value     float64
}

type Sender struct {
	svID       string
	siteCount  int
	itemCount  int
	carbonDest string
	relayDest  string
	timeout    time.Duration
	rnd        *rand.Rand
}

func main() {
	carbonDest := flag.String("carbon-dest", "127.0.0.1:12003", "go-carbon destination")
	relayDest := flag.String("relay-dest", "127.0.0.1:2003", "carbon-relay-ng destination")
	timeout := flag.Duration("timeout", 10*time.Second, "timeout")
	svCount := flag.Int("sv-count", 30, "source server count")
	siteCount := flag.Int("site-count", 100, "site count")
	itemCount := flag.Int("item-count", 5, "item count per site")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := run(*carbonDest, *relayDest, *timeout, *svCount, *siteCount, *itemCount); err != nil {
		log.Fatal(err)
	}
}

func run(carbonDest, relayDest string, timeout time.Duration, svCount, siteCount, itemCount int) error {
	var g errgroup.Group
	for i := 0; i < svCount; i++ {
		svID := fmt.Sprintf("sv%02d", i)
		g.Go(func() error {
			s := newSender(svID, siteCount, itemCount, carbonDest, relayDest, timeout)
			return s.run()
		})
	}
	return g.Wait()
}

func newSender(svID string, siteCount, itemCount int, carbonDest, relayDest string, timeout time.Duration) *Sender {
	return &Sender{
		svID:       svID,
		siteCount:  siteCount,
		itemCount:  itemCount,
		carbonDest: carbonDest,
		relayDest:  relayDest,
		timeout:    timeout,
		rnd:        rand.New(rand.NewSource(newRandSeed())),
	}
}

func (s *Sender) run() error {
	for {
		now := time.Now()
		targetTime := now.Truncate(time.Minute)
		durTillNextMin := targetTime.Add(time.Minute).Sub(now)
		time.Sleep(durTillNextMin)

		metrics := genRandMetrics(targetTime, s.siteCount, s.itemCount, s.svID, s.rnd)
		data := encodeMetrics(metrics)
		//log.Printf("data=%s", data)
		var g errgroup.Group
		g.Go(func() error {
			return s.send(s.carbonDest, data)
		})
		g.Go(func() error {
			return s.send(s.relayDest, data)
		})
		if err := g.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func genRandMetrics(t time.Time, siteCount, itemCount int, svID string, rnd *rand.Rand) []Metric {
	tstamp := t.Unix()

	metrics := make([]Metric, siteCount*itemCount)
	for i := 0; i < siteCount; i++ {
		for j := 0; j < itemCount; j++ {
			metrics[i*itemCount+j] = Metric{
				item:      fmt.Sprintf("site%04d.item%02d.%s", i, j, svID),
				value:     float64(rnd.Intn(100)),
				timestamp: tstamp,
			}
		}
	}
	return metrics
}

func encodeMetrics(metrics []Metric) []byte {
	var b strings.Builder
	for _, m := range metrics {
		fmt.Fprintf(&b, "%s %s %d\n",
			m.item,
			strconv.FormatFloat(m.value, 'f', -1, 64),
			m.timestamp)
	}
	return []byte(b.String())
}

func (s *Sender) send(dest string, data []byte) error {
	d := net.Dialer{Timeout: s.timeout}
	conn, err := d.Dial("tcp", dest)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.Write(data); err != nil {
		return err
	}

	return nil
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}
