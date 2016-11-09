package main

import (
	"fmt"
	"os"
	"bufio"
	"net/http"
	"log"
	"io"
	"time"
	"net"
)

var OdklTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}


func main() {

	parallel := 64

	stream := make(chan Img, parallel)
	eof    := make(chan int)
	finished := make(chan int)


	for i := 0; i < parallel; i++ {
		go func() {
			httpClient := &http.Client{
				Transport: OdklTransport,
				CheckRedirect: func(req *http.Request, via []*http.Request) error {return http.ErrUseLastResponse;},
			};

			Loop:
			for {
				select {
				case img := <- stream:
					//fmt.Printf("Downloading %d: %x, %s\n", img.num, img.id, img.uri)
					downloadImg(img, httpClient)
					//fmt.Printf("Downloaded %d: %x, %s\n", img.num, img.id, img.uri)
				case <- eof:
					//fmt.Printf("Received eof\n")
					break Loop
				}
			}
			finished <- 0
		}();
	}


	fileName := "./avas.log"
	file, err := os.Open(fileName)
	if (err != nil) {
		fmt.Printf("Failed to open file %s: %s\n", fileName, err)
		return
	}

	reader := bufio.NewReader(file);

	var num int = 0
	for {
		var (
			id int64
			uri string
		)
		n, err := fmt.Fscanln(reader, &id, &uri);
		if n == 0 || err != nil {
			fmt.Printf("Found strange line %d, %s\n", n, err)
			break
		}

		stream <- Img{
			num: num,
			id: id,
			uri: uri,
		}
		num++
		if (num % 1000 == 0) {
			fmt.Printf("Downloaded %d files\n", num)
		}
	}
	fmt.Printf("Read all %d files\n", num)
	for i := 0; i < parallel; i++ {
		eof <- 0
	}
	for i := 0; i < parallel; i++ {
		<- finished
	}
}


func downloadImg(img Img, httpClient *http.Client) {
	httpResponse, err := httpClient.Get(img.uri)

	if (err != nil) {
		log.Printf("Received err response %s\n", err)
		return
	}

	defer httpResponse.Body.Close()

	if (httpResponse.StatusCode != 200) {
		log.Printf("Received non-2xx response %s\n", httpResponse.StatusCode)
		return
	}

	outFile, err := openImgFile(img.num)

	if (err != nil) {
		log.Printf("Cannot open file for writing %s\n", err)
		return
	}

	defer outFile.Close()

	_, err = io.Copy(outFile, httpResponse.Body)

	if (err != nil) {
		log.Printf("Cannot transfer data from response to file %s\n", err)
		return
	}
}


func openImgFile(num int) (*os.File, error) {

	filesPerFolder := 0x2000

	err := os.MkdirAll(fmt.Sprintf("./images/%06x", num / filesPerFolder), 0777)
	if (err != nil) {
		return nil, err
	}

	fileName := fmt.Sprintf("./images/%06x/%05x.jpg", num / filesPerFolder, num % filesPerFolder)
	return os.Create(fileName)
}

type Img struct {
	num int
	id int64
	uri string
}
