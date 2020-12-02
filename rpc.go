/*
 * This programs demonstrates RPC in go.
 * Server is represented as a goroutine.
 */

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Constants used to indicate RPC status.
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

// RPC specification.
type PutArgs struct {
	Key string
	Val string
}

type PutReply struct {
	Err string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Val string
	Err string
}

// Server
type KV struct {
	mtx  sync.Mutex
	data map[string]string
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()

	kv.data[args.Key] = args.Val
	reply.Err = OK
	return nil
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Val = val
	} else {
		reply.Err = ErrNoKey
		reply.Val = ""
	}
	return nil
}

func launchServer() {
	kv := new(KV)
	kv.data = map[string]string{}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("Listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
}

// Client
func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("Connect server error:", err)
	}
	return client
}

func put(key string, val string) {
	client := connect()
	defer client.Close()
	args := PutArgs{key, val}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("Client put error:", err)
	}
}

func get(key string) string {
	client := connect()
	defer client.Close()
	args := GetArgs{key}
	reply := GetReply{}
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("Client get error:", err)
	}
	return reply.Val
}

func main() {
	launchServer()
	put("key", "value")
	fmt.Printf("Put kv pair %s : %s done\n", "key", "value")
	fmt.Printf("Get(%s) -> %s\n", "key", get("key"))
}
