package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func listenConnections(write_only_channel chan<- []byte, Host string, Port string) {

	serverAddress := Host + ":" + Port
	fmt.Println("Starting" + "TCP" + "server on" + serverAddress)

	listener, err := net.Listen("tcp", Host+":"+Port)
	if err != nil {
		log.Fatal("Error raised when listening:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("listen complete")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Error raised(not accept):", err)
		}
		go handleConnection(conn, write_only_channel)
	}
}

func handleConnection(conn net.Conn, write_only_channel chan<- []byte) {

	for {
		buffer := make([]byte, 101)
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Fatal("Error raised when reading bytes:", err)
			os.Exit(1)
		}
		buffer = buffer[:bytesRead]
		write_only_channel <- buffer
		fmt.Println(buffer)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)
	serverLenth := len(scs.Servers)

	// Set up the server
	channel := make(chan []byte)
	go listenConnections(channel, scs.Servers[serverId].Host, scs.Servers[serverId].Port)
	conncetionRecord := make([]net.Conn, serverLenth)
	fmt.Println("finsihed make connection record")
	time.Sleep(2 * time.Second)

	for idx := 0; idx < len(scs.Servers); idx++ {
		conn, err := net.Dial("tcp", scs.Servers[idx].Host+":"+scs.Servers[idx].Port)

		if err != nil {
			for j := 0; j < 10; j++ {
				conn, err = net.Dial("tcp", scs.Servers[idx].Host+":"+scs.Servers[idx].Port)
			}
		}

		if err != nil {
			log.Fatal("Error rasied when dialing:", err)
		} else {
			fmt.Println("Successfully connect to:", scs.Servers[idx].Host, scs.Servers[idx].Port)
		}

		conncetionRecord[idx] = conn
		// fmt.Println("Connected to server %d from server %d", idx, serverId)
	}

	// Read the input file
	numServer := len(scs.Servers)
	readFile, err := os.Open(os.Args[2])
	if err != nil {
		log.Fatal("Error raised when reading file:", err)
	}

	// Partition the records
	serverBits := int(math.Log2(float64(numServer)))
	for {
		buffer := make([]byte, 101)
		buffer[0] = 0
		_, err := readFile.Read(buffer[1:])
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
		}
		if err == io.EOF {
			break
		}
		aimServerId := int(buffer[1] >> (8 - serverBits))
		tempConn := conncetionRecord[aimServerId]
		fmt.Println("writing to ", aimServerId)
		tempConn.Write(buffer)
	}
	readFile.Close()

	// Send relevent records to peer server
	for i := 0; i < serverLenth; i++ {
		completeSign := make([]byte, 101)
		completeSign[0] = 1
		conn := conncetionRecord[i]
		conn.Write(completeSign)
	}

	// Read from channel
	lst := [][]byte{}
	completedServerNumber := 0
	for {
		channelContent := <-channel
		if channelContent[0] == 1 {
			completedServerNumber++
			if completedServerNumber == len(scs.Servers) {
				break
			}
			continue
		} //complete

		// fmt.Println(channelContent[1:])
		lst = append(lst, channelContent[1:])
	}

	sort.Slice(lst, func(i, j int) bool {
		return bytes.Compare(lst[i][:10], lst[j][:10]) < 0
	})

	write, error := os.Create(os.Args[3])
	if error != nil {
		log.Fatal("Error raised when creating:", error)
	}
	defer write.Close()

	writer := bufio.NewWriter(write)
	for _, j := range lst {
		_, err := writer.Write(j)
		if err != nil {
			log.Fatal("Error raised when writing:", err)
		}
	}

	writer.Flush()
}
