package main

import (
	"net"
	"log"
	"bufio"
	"net/http"
	"sync"
	"io"
	"strconv"
	"time"
)

type Backend struct {
	net.Conn //Embedded type - lets you call methods on Backend that will get called on net.Conn
	Reader *bufio.Reader
	Writer *bufio.Writer
}

var backendQueue chan *Backend	//channel that holds Backend Types
var requestBytes map[string]int64
var requestLock sync.Mutex

//Init Runs when execution starts - these variables need to be created to be used
func init() {
	log.Printf("This init creates some stuff!!")
	requestBytes = make(map[string]int64)
	backendQueue = make(chan *Backend, 10) //Buffered channel -> can shuv buffered amount
	//of data in channel and continue on
}

func init() {
	log.Printf("This init is just a test!!")
}

func main() {

	var waitGroup sync.WaitGroup

	waitGroup.Add(2)
	go listenAndServeOnPort(":8081")
	go listenAndRespondOn(":8080")

	waitGroup.Wait()

}


func listenAndServeOnPort(port string) {
	log.Printf("Listening On Port: %s\n", port)
	http.ListenAndServe(port, http.FileServer(http.Dir("./docroot")))
}

//Server
func listenAndRespondOn(port string) {
	log.Printf("Listening and Responding On Port: %s\n", port)
	//listen on this port with the tcp network protocol
	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	for {
		//Waiting here for next connection - Blocked
		log.Printf("Blocked here, waiting....\n")
		if conn, err := l.Accept(); err == nil {
			go handleConnetion(conn)
		}
	}
}


func handleConnetion(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Failed to read request: %s", err)
			}
			return
		}
		be, err := getBackend()
		if err != nil {
			return
		}

		if err := req.Write(be.Writer); err == nil {
			be.Writer.Flush()
		}

		go queueBackend(be)


		if resp, err := http.ReadResponse(be.Reader, req); err == nil {
			bytes := updateStats(req, resp)
			resp.Header.Set("X-Bytes", strconv.FormatInt(bytes, 10))

			resp.Close = true
			if err := resp.Write(conn); err == nil {
				log.Printf("%s: %s %d", req.URL.Path, req.Host, resp.StatusCode)
			}
			conn.Close()
		}


	}
}

//Lock and unlock when this function completes
func updateStats(req *http.Request, resp *http.Response) int64 {
	requestLock.Lock()
	defer requestLock.Unlock()

	//referencing a key that doesnt exist gives back the zero value
	bytes := requestBytes[req.URL.Path] + resp.ContentLength
	requestBytes[req.URL.Path] = bytes
	return bytes
}

func getBackend() (*Backend, error) {
	//Select is interesting...it blocks until it hits a true case
	select {
	//Try to get a backed out of this channel - if there is nothing in the channel you block
	//We want to wait a bit for another backend to maybe become available
	//The timer fires and after a certain amount of time, if any case becomes true (only one other
	//in this case), select will choose that case.  If the timer expires that case is selected
	case be := <-backendQueue:
		return be, nil
	case <-time.After(100 * time.Millisecond):
		be, err := net.Dial("tcp", "127.0.0.1:8081")
		if err != nil {
			return nil, err
		}

		return &Backend{
			Conn: be,
			Reader: bufio.NewReader(be),
			Writer: bufio.NewWriter(be),
		}, nil
	}
}


func queueBackend(be *Backend) {
	select {
	//Backends die off if you can queue them after 1 second
	case backendQueue <- be:
		// Backend re-enqueued safely, move on.
	case <-time.After(1 * time.Second):
		be.Close()
	}

}

