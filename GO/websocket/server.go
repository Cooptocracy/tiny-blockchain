package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"flag"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:1024,
		WriteBufferSize:1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func wsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		conn *websocket.Conn
		err  error
		data []byte
	)
	conn, err = upgrader.Upgrade(w, r, nil)

	if err != nil {
		return
	}
	for {
		_, data, err = conn.ReadMessage() 
		if err != nil {
			goto ERR
		}
    fmt.Printf("%s \n", []byte(data))
		err = conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			goto ERR
		}
	}
ERR:
	conn.Close()
}

var addr = flag.String("addr", ":8888", "http service address")
func main() {
	fmt.Println("websocket is start ...")
	http.HandleFunc("/ws", wsHandler)

	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
