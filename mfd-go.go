package main

import (
	"fmt"
	"os"
	"bufio"
	"net/http"
	"log"
	"io"
)

func main() {

	stream := make(chan Img, 256)
	eof    := make(chan int)

	for i := 0; i < 16; i++ {
		go func() {
			httpClient := &http.Client{};
			for {
				select {
				case img := <- stream:
					downloadImg(img, httpClient)
				case <- eof:
					break
				}

			}
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
			id int
			uri string
		)
		n, err := fmt.Fscanln(reader, &id, &uri);
		if n == 0 || err != nil {
			break
		}

		stream <- Img{
			num: num,
			id: id,
			uri: uri,
		}
		num++
	}
	eof <- 0
}


func downloadImg(img Img, httpClient *http.Client) {
	httpResponse, err := httpClient.Get(img.uri)

	if (err != nil) {
		log.Printf("Received err response %s\n", err)
		return
	}

	defer httpResponse.Body.Close()

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

	err := os.Mkdir(fmt.Sprintf("./images/%06x", num / filesPerFolder), 0666)
	if (err != nil) {
		return nil, err
	}

	fileName := fmt.Sprintf("./images/%06x/%05x", num / filesPerFolder, num % filesPerFolder)
	return os.Create(fileName)
}

type Img struct {
	num int
	id int
	uri string
}
