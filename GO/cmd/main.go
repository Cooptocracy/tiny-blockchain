package main

import "github.com/Cooptocracy/tiny-blockchain/core"



func main(){
	bc := core.NewBlockchain();
	bc.SendData("Send 1 MTC to Newmen")
	bc.SendData("Send 1 LEX to Headly")
	bc.Print()
}
