package websocket

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func newPipeConnPair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	server := newConn(serverSide, true, 1024, 1024, nil, nil, nil)
	client := newConn(clientSide, false, 1024, 1024, nil, nil, nil)
	t.Cleanup(func() {
		serverSide.Close()
		clientSide.Close()
	})
	return server, client
}

func TestCloseHandshake(t *testing.T) {
	server, client := newPipeConnPair(t)

	clientResult := make(chan error, 1)
	go func() {
		_, _, err := client.NextReader()
		clientResult <- err
	}()

	start := time.Now()
	if err := server.Close(); err != nil {
		t.Fatalf("Close() returned %v", err)
	}
	if elapsed := time.Since(start); elapsed > defaultCloseTimeout {
		t.Fatalf("Close() took %v, want no more than %v", elapsed, defaultCloseTimeout)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("second Close() returned %v", err)
	}

	err := <-clientResult
	closeErr, ok := err.(*CloseError)
	if !ok {
		t.Fatalf("client read returned %T %v, want *CloseError", err, err)
	}
	if closeErr.Code != CloseNormalClosure {
		t.Fatalf("close code = %d, want %d", closeErr.Code, CloseNormalClosure)
	}

	if err := server.WriteMessage(TextMessage, []byte("late")); !errors.Is(err, ErrCloseSent) {
		t.Fatalf("WriteMessage() after Close returned %v, want %v", err, ErrCloseSent)
	}
	if _, _, err := server.NextReader(); !IsCloseError(err, CloseNormalClosure) {
		t.Fatalf("NextReader() after Close returned %v, want close code %d", err, CloseNormalClosure)
	}
}

func TestCloseRejectsWritesWhileWaitingForAck(t *testing.T) {
	server, client := newPipeConnPair(t)
	closeFrameSeen := make(chan struct{})
	ackAllowed := make(chan struct{})

	client.SetCloseHandler(func(code int, text string) error {
		close(closeFrameSeen)
		<-ackAllowed
		return client.WriteControl(CloseMessage, FormatCloseMessage(code, ""), time.Now().Add(writeWait))
	})
	go func() {
		_, _, _ = client.NextReader()
	}()

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- server.Close()
	}()

	select {
	case <-closeFrameSeen:
	case <-time.After(defaultCloseTimeout):
		t.Fatal("client did not receive the close frame")
	}

	if err := server.WriteMessage(TextMessage, []byte("late")); !errors.Is(err, ErrCloseSent) {
		t.Fatalf("WriteMessage() while closing returned %v, want %v", err, ErrCloseSent)
	}

	close(ackAllowed)
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close() returned %v", err)
		}
	case <-time.After(defaultCloseTimeout):
		t.Fatal("Close() did not return after the peer acknowledged")
	}
}

func TestCloseTimesOutWithoutAck(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	server := newConn(serverSide, true, 1024, 1024, nil, nil, nil)
	server.closeTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		serverSide.Close()
		clientSide.Close()
	})

	frame := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(clientSide, buf); err != nil {
			close(frame)
			return
		}
		frame <- buf
	}()

	start := time.Now()
	err := server.Close()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Close() took %v with a %v timeout", elapsed, server.closeTimeout)
	}
	if !errors.Is(err, errCloseTimeout) {
		t.Fatalf("Close() returned %v, want %v", err, errCloseTimeout)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Close() error does not report a timeout: %v", err)
	}

	buf, ok := <-frame
	if !ok {
		t.Fatal("close frame was not sent")
	}
	if buf[0] != byte(CloseMessage)|finalBit || buf[1] != 2 {
		t.Fatalf("close frame header = %#v, want close frame with length 2", buf[:2])
	}
	if int(buf[2])<<8|int(buf[3]) != CloseNormalClosure {
		t.Fatalf("close frame code = %d, want %d", int(buf[2])<<8|int(buf[3]), CloseNormalClosure)
	}
}
