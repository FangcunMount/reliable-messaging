package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

const maxFrame = 16 << 20

var errDrop = errors.New("owned fixture connection dropped")

type packetPeer struct {
	client, server net.Conn
	once           sync.Once
	mu             sync.Mutex
	business       bool
	pending        string
}

func (p *packetPeer) close() { p.once.Do(func() { p.client.Close(); p.server.Close() }) }
func (f *injector) startPackets(listener net.Listener) func() error {
	var mu sync.Mutex
	peers := map[*packetPeer]bool{}
	closing := false
	slots := make(chan struct{}, 64)
	var wg sync.WaitGroup
	var closeOnce sync.Once
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case slots <- struct{}{}:
			default:
				client.Close()
				continue
			}
			server, err := net.DialTimeout("tcp", f.upstream, 2*time.Second)
			if err != nil {
				client.Close()
				<-slots
				continue
			}
			peer := &packetPeer{client: client, server: server}
			mu.Lock()
			if closing {
				mu.Unlock()
				peer.close()
				<-slots
				return
			}
			peers[peer] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { peer.close(); mu.Lock(); delete(peers, peer); mu.Unlock(); <-slots }()
				clientDone := make(chan struct{})
				go func() { defer close(clientDone); defer peer.close(); _ = f.pipeClient(peer) }()
				_ = f.pipeServer(peer)
				peer.close()
				<-clientDone
			}()
		}
	}()
	return func() error {
		closeOnce.Do(func() {
			listener.Close()
			mu.Lock()
			closing = true
			for peer := range peers {
				peer.close()
			}
			mu.Unlock()
			wg.Wait()
			emit("closed", nil)
		})
		return nil
	}
}
func readFrame(reader io.Reader, kind string) ([]byte, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(reader, head); err != nil {
		return nil, err
	}
	n := 0
	if kind == "mysql" {
		n = int(head[0]) | int(head[1])<<8 | int(head[2])<<16
	} else {
		n = int(int32(binary.LittleEndian.Uint32(head))) - 4
	}
	if n < 0 || n > maxFrame {
		return nil, errors.New("invalid bounded frame")
	}
	out := make([]byte, n+4)
	copy(out, head)
	_, err := io.ReadFull(reader, out[4:])
	return out, err
}
func writeFrame(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}
func (f *injector) pipeClient(p *packetPeer) error {
	for {
		frame, err := readFrame(p.client, f.kind)
		if err != nil {
			return err
		}
		if f.kind == "mysql" {
			err = f.mysqlClient(p, frame)
		} else {
			err = f.mongoClient(p, frame)
		}
		if err != nil {
			return err
		}
		if err = writeFrame(p.server, frame); err != nil {
			return err
		}
	}
}
func (f *injector) pipeServer(p *packetPeer) error {
	for {
		frame, err := readFrame(p.server, f.kind)
		if err != nil {
			return err
		}
		p.mu.Lock()
		pending := p.pending
		p.pending = ""
		p.mu.Unlock()
		if pending != "" {
			success := f.kind == "mysql" && len(frame) > 4 && frame[4] == 0
			if f.kind == "mongo" {
				doc, _ := mongoMessage(frame)
				success = mongoOK(doc)
			}
			if success {
				f.injected("after", true)
				return errDrop
			}
		}
		if err = writeFrame(p.client, frame); err != nil {
			return err
		}
	}
}
func (f *injector) mysqlClient(p *packetPeer, frame []byte) error {
	if len(frame) < 5 || (frame[4] != 3 && frame[4] != 0x16) {
		return nil
	}
	command := strings.ToLower(strings.Join(strings.Fields(string(frame[5:])), " "))
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.HasPrefix(command, "update rm_tx_execution set state='committed'") {
		p.business = true
	}
	if strings.TrimSuffix(command, ";") == "rollback" {
		p.business = false
	}
	if strings.TrimSuffix(command, ";") != "commit" || !p.business {
		return nil
	}
	p.business = false
	boundary := f.target("commit")
	if boundary == "before" {
		f.injected("before", false)
		return errDrop
	}
	if boundary == "after" {
		p.pending = "commit"
	}
	return nil
}

// Parse only bounded, uncompressed OP_MSG documents and document sequences.
// Initial legacy hello and unrelated frames pass through unchanged.
func mongoMessage(frame []byte) (bson.Raw, []bson.Raw) {
	if len(frame) < 21 || binary.LittleEndian.Uint32(frame[12:16]) != 2013 {
		return nil, nil
	}
	if binary.LittleEndian.Uint32(frame[16:20])&1 != 0 {
		if len(frame) < 25 {
			return nil, nil
		}
		frame = frame[:len(frame)-4]
	}
	rest := frame[20:]
	var main bson.Raw
	var updates []bson.Raw
	for len(rest) > 0 {
		kind := rest[0]
		rest = rest[1:]
		if len(rest) < 4 {
			return nil, nil
		}
		n := int(int32(binary.LittleEndian.Uint32(rest)))
		if n < 5 || n > len(rest) {
			return nil, nil
		}
		if kind == 0 {
			doc := bson.Raw(rest[:n])
			if doc.Validate() != nil {
				return nil, nil
			}
			main = doc
			rest = rest[n:]
			continue
		}
		if kind != 1 {
			return nil, nil
		}
		section := rest[4:n]
		zero := bytes.IndexByte(section, 0)
		if zero < 0 {
			return nil, nil
		}
		name := string(section[:zero])
		documents := section[zero+1:]
		for len(documents) >= 5 {
			size := int(int32(binary.LittleEndian.Uint32(documents)))
			if size < 5 || size > len(documents) {
				return nil, nil
			}
			doc := bson.Raw(documents[:size])
			if doc.Validate() != nil {
				return nil, nil
			}
			if name == "updates" {
				updates = append(updates, doc)
			}
			documents = documents[size:]
		}
		rest = rest[n:]
	}
	return main, updates
}
func mongoOK(doc bson.Raw) bool {
	if doc == nil || doc.Lookup("writeConcernError").Type != 0 {
		return false
	}
	v := doc.Lookup("ok")
	if n, ok := v.DoubleOK(); ok {
		return n == 1
	}
	if n, ok := v.Int32OK(); ok {
		return n == 1
	}
	return false
}
func (f *injector) mongoClient(p *packetPeer, frame []byte) error {
	doc, updates := mongoMessage(frame)
	if doc == nil {
		return nil
	}
	lsid, ok := doc.Lookup("lsid").DocumentOK()
	if !ok {
		return nil
	}
	number, ok := doc.Lookup("txnNumber").Int64OK()
	if !ok {
		return nil
	}
	identity := sessionKey(lsid, number)
	collection, _ := doc.Lookup("update").StringValueOK()
	if collection == "rm_tx_execution" {
		if array, ok := doc.Lookup("updates").ArrayOK(); ok {
			values, err := array.Values()
			if err != nil {
				return err
			}
			for _, v := range values {
				if d, ok := v.DocumentOK(); ok {
					updates = append(updates, d)
				}
			}
		}
		for _, update := range updates {
			u, ok := update.Lookup("u").DocumentOK()
			if !ok {
				continue
			}
			set, ok := u.Lookup("$set").DocumentOK()
			if !ok {
				continue
			}
			state, _ := set.Lookup("state").StringValueOK()
			if state == "committed" {
				f.mu.Lock()
				if f.boundary != "" && len(f.sessions) < 64 {
					f.sessions[identity] = append(bson.Raw(nil), lsid...)
				}
				f.mu.Unlock()
			}
		}
	}
	if doc.Lookup("commitTransaction").Type == 0 {
		return nil
	}
	f.mu.Lock()
	_, target := f.sessions[identity]
	boundary := f.boundary
	if target {
		f.seen++
	}
	f.mu.Unlock()
	if !target {
		return nil
	}
	if boundary == "before" {
		f.injected("before", false)
		return errDrop
	}
	if boundary == "after" {
		p.mu.Lock()
		p.pending = identity
		p.mu.Unlock()
	}
	return nil
}
