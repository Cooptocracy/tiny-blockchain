package main

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
)

func main() {
	// connection
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer conn.Close()

	// write  json data
	key := "school"
	m := map[string]string{"username": "coop.blocks", "mobile": "1123342162"}
	value, _ := json.Marshal(m)

	res, err := conn.Do("SETNX", key, value)
	if err != nil {
		fmt.Println("redis set fail", err)
	}

	if res == int64(1) {
		fmt.Println("set json success")
	}

	// redis json data
	m2 := map[string]string{}
	value, err = redis.Bytes(conn.Do("GET", key))
	if err != nil {
		fmt.Println("read json fail ", err)
	}
	err = json.Unmarshal(value, &m2)
	if err != nil {
		fmt.Println("json format fail", err)
	}
	fmt.Println("username is ", m2["username"])
	fmt.Println("mobile is ", m2["mobile"])
}
