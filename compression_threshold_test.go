// Copyright 2023 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// TestCompressionThresholdAlternating sends a small and a large message back
// to back on the same connection. The small message must skip compression
// while the large one is still compressed, and the receiver must be able to
// read both interleaved frames on the same stream.
func TestCompressionThresholdAlternating(t *testing.T) {
	const threshold = 64

	small := []byte("hi") // well below the threshold
	large := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog. ", 8))

	for _, isServer := range []bool{true, false} {
		name := "server"
		if !isServer {
			name = "client"
		}
		t.Run(name, func(t *testing.T) {
			var wire bytes.Buffer
			wc := newTestConn(nil, &wire, isServer)
			wc.newCompressionWriter = compressNoContextTakeover
			wc.newDecompressionReader = decompressNoContextTakeover
			wc.SetCompressionThreshold(threshold)

			rc := newTestConn(bufio.NewReader(&wire), nil, !isServer)
			rc.newDecompressionReader = decompressNoContextTakeover

			steps := []struct {
				data     []byte
				compress bool
			}{
				{small, false},
				{large, true},
				{large, true},
				{small, false},
			}
			for _, step := range steps {
				start := wire.Len()
				if err := wc.WriteMessage(TextMessage, step.data); err != nil {
					t.Fatalf("WriteMessage(%d bytes): %v", len(step.data), err)
				}
				frame := wire.Bytes()[start:]
				if got := frame[0]&rsv1Bit != 0; got != step.compress {
					t.Errorf("message of %d bytes: rsv1=%v, want %v", len(step.data), got, step.compress)
				}

				mt, got, err := rc.ReadMessage()
				if err != nil {
					t.Fatalf("ReadMessage: %v", err)
				}
				if mt != TextMessage || !bytes.Equal(got, step.data) {
					t.Fatalf("round trip mismatch: type=%d len=%d want len=%d", mt, len(got), len(step.data))
				}
			}
		})
	}
}

func TestCompressionThresholdBoundary(t *testing.T) {
	const threshold = 32

	var wire bytes.Buffer
	c := newTestConn(nil, &wire, true)
	c.newCompressionWriter = compressNoContextTakeover
	c.SetCompressionThreshold(threshold)

	payload := make([]byte, threshold)
	if err := c.WriteMessage(BinaryMessage, payload); err != nil {
		t.Fatalf("WriteMessage at threshold: %v", err)
	}
	if wire.Bytes()[0]&rsv1Bit != 0 {
		t.Errorf("message equal to threshold (%d bytes) must not be compressed", threshold)
	}

	wire.Reset()
	payload = append(payload, 0)
	if err := c.WriteMessage(BinaryMessage, payload); err != nil {
		t.Fatalf("WriteMessage above threshold: %v", err)
	}
	if wire.Bytes()[0]&rsv1Bit == 0 {
		t.Errorf("message above threshold (%d bytes) must be compressed", len(payload))
	}
}

func TestCompressionThresholdDefaultCompressesAll(t *testing.T) {
	var wire bytes.Buffer
	c := newTestConn(nil, &wire, true)
	c.newCompressionWriter = compressNoContextTakeover

	if got := c.CompressionThreshold(); got != 0 {
		t.Fatalf("default threshold = %d, want 0", got)
	}

	if err := c.WriteMessage(TextMessage, []byte("x")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if wire.Bytes()[0]&rsv1Bit == 0 {
		t.Error("default threshold must keep compressing tiny messages")
	}

	// A negative threshold is normalized to zero.
	c.SetCompressionThreshold(-5)
	if got := c.CompressionThreshold(); got != 0 {
		t.Errorf("negative threshold normalized to %d, want 0", got)
	}
}

// TestCompressionThresholdNoExtraAlloc ensures the threshold check itself
// does not add per-message allocations. A message below the threshold takes
// the allocation-free fast path, while a compressed message must not allocate
// more than the pre-existing baseline (threshold disabled) compression path.
func TestCompressionThresholdNoExtraAlloc(t *testing.T) {
	var wire bytes.Buffer
	small := []byte("tiny control-like payload")
	large := make([]byte, 512)

	thresholdConn := newTestConn(nil, &wire, true)
	thresholdConn.newCompressionWriter = compressNoContextTakeover
	thresholdConn.SetCompressionThreshold(128)

	allocsSmall := testing.AllocsPerRun(100, func() {
		wire.Reset()
		if err := thresholdConn.WriteMessage(TextMessage, small); err != nil {
			t.Fatalf("WriteMessage small: %v", err)
		}
	})
	if allocsSmall != 0 {
		t.Errorf("sub-threshold message allocs = %.2f, want 0", allocsSmall)
	}

	allocsLarge := testing.AllocsPerRun(100, func() {
		wire.Reset()
		if err := thresholdConn.WriteMessage(TextMessage, large); err != nil {
			t.Fatalf("WriteMessage large: %v", err)
		}
	})

	baselineConn := newTestConn(nil, &wire, true)
	baselineConn.newCompressionWriter = compressNoContextTakeover
	allocsBaseline := testing.AllocsPerRun(100, func() {
		wire.Reset()
		if err := baselineConn.WriteMessage(TextMessage, large); err != nil {
			t.Fatalf("baseline WriteMessage large: %v", err)
		}
	})

	if allocsLarge > allocsBaseline {
		t.Errorf("compressed message allocs = %.2f, baseline = %.2f; threshold check must not allocate", allocsLarge, allocsBaseline)
	}
}
