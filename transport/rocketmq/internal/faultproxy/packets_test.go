package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func mysqlFrame(command string) []byte {
	body := append([]byte{3}, []byte(command)...)
	out := make([]byte, 4)
	out[0] = byte(len(body))
	out[1] = byte(len(body) >> 8)
	out[2] = byte(len(body) >> 16)
	return append(out, body...)
}
func TestMySQLOnlyTargetsOriginalWitnessCommit(t *testing.T) {
	f := &injector{kind: "mysql", boundary: "before", nth: 1}
	peer := &packetPeer{}
	if err := f.mysqlClient(peer, mysqlFrame("COMMIT")); err != nil {
		t.Fatal("protocol bookkeeping commit targeted")
	}
	if f.seen != 0 {
		t.Fatal("unmarked transaction targeted")
	}
	if err := f.mysqlClient(peer, mysqlFrame("UPDATE rm_tx_execution SET state='committed' WHERE execution_key=?")); err != nil {
		t.Fatal(err)
	}
	if err := f.mysqlClient(peer, mysqlFrame("COMMIT")); !errors.Is(err, errDrop) {
		t.Fatal("original transaction commit not intercepted", err)
	}
	if f.dropped != 1 || f.acks != 0 {
		t.Fatal("before-commit fault invented a server acknowledgment")
	}
}
func opMsg(main bson.D, updates ...bson.D) []byte {
	doc, _ := bson.Marshal(main)
	body := append([]byte{0, 0, 0, 0, 0}, doc...)
	if len(updates) > 0 {
		section := append([]byte{0, 0, 0, 0}, append([]byte("updates"), 0)...)
		for _, update := range updates {
			raw, _ := bson.Marshal(update)
			section = append(section, raw...)
		}
		binary.LittleEndian.PutUint32(section, uint32(len(section)))
		body = append(body, 1)
		body = append(body, section...)
	}
	frame := make([]byte, 16)
	binary.LittleEndian.PutUint32(frame, uint32(16+len(body)))
	binary.LittleEndian.PutUint32(frame[12:], 2013)
	return append(frame, body...)
}
func TestMongoTracksExactSessionAndTransactionAcrossConnections(t *testing.T) {
	lsid := bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: bytes.Repeat([]byte{1}, 16)}}}
	frame := opMsg(bson.D{{Key: "update", Value: "rm_tx_execution"}, {Key: "lsid", Value: lsid}, {Key: "txnNumber", Value: int64(7)}}, bson.D{{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: "committed"}}}}}})
	f := &injector{kind: "mongo", boundary: "before", nth: 1, sessions: map[string]bson.Raw{}}
	if err := f.mongoClient(&packetPeer{}, frame); err != nil {
		t.Fatal(err)
	}
	if len(f.sessions) != 1 {
		t.Fatal("original witness update session not captured")
	}
	unrelated := opMsg(bson.D{{Key: "commitTransaction", Value: 1}, {Key: "lsid", Value: lsid}, {Key: "txnNumber", Value: int64(8)}})
	if err := f.mongoClient(&packetPeer{}, unrelated); err != nil {
		t.Fatal("other transaction intercepted")
	}
	commit := opMsg(bson.D{{Key: "commitTransaction", Value: 1}, {Key: "lsid", Value: lsid}, {Key: "txnNumber", Value: int64(7)}})
	if err := f.mongoClient(&packetPeer{}, commit); !errors.Is(err, errDrop) {
		t.Fatal("commit on another connection escaped exact session fence")
	}
	if f.dropped != 1 || f.acks != 0 {
		t.Fatal("uncommitted outcome invented server ack")
	}
}
