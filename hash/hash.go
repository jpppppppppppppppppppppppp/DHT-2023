package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

func gethash(str string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(str))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}

func main() {
	str := "jy"
	fmt.Println(*gethash(str))
}
