package main

import (
	"io"
	"encoding/json"
	"net/http"
	"github.com/Cooptocracy/tiny-blockchain/core"
)

var blockchain *core.Blockchain 

func run(){
	http.HandleFunc("/blockchain/get",blockchainGetHanle)
	http.HandleFunc("/blockchain/write",blockchainWriteHanle)
	http.ListenAndServe("localhost:8888",nil)
}

func blockchainGetHanle(w http.ResponseWriter,r *http.Request){
	bytes, error := json.Marshal(blockchain)
	if error != nil {
		http.Error(w,error.Error(),http.StatusInternalServerError)
		return
	}
	io.WriteString(w,string(bytes))
}

func blockchainWriteHanle(w http.ResponseWriter,r *http.Request){
	blockData := r.URL.Query().Get("data")
	blockchain.SendData(blockData)
	blockchainGetHanle(w,r) 
}

func main(){
	blockchain = core.NewBlockchain()
	run()
}
