package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

// connection redis
func connDB() (c redis.Conn, err error) {
	db, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	return db, err
}

func main() {
	// connection
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer conn.Close()

	// write data
	_, err = conn.Do("SET", "Language", "Golang", "EX", 10)  _, err :=
	if err != nil {
		fmt.Println("redis set fail", err)
		return
	}

	time.Sleep(2 * time.Second)
	// read data
	data, err := redis.String(conn.Do("GET", "Language"))
	if err != nil {
		fmt.Println("read redis is fail", err)
	}
	fmt.Printf("GET Values %s \n", data)

	// check is exits
	is_exits, err := redis.Bool(conn.Do("EXISTS", "Language"))
	if err != nil {
		fmt.Println("redis is panin", err)
	} else {
		fmt.Printf("exists or not: %v \n", is_exits)
	}

}
