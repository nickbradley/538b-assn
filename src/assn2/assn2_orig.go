package main

import (
  "os"
  "log"
  "net"
  "time"
  //"flag"
  //"io/ioutil"
  "bufio"
  "strconv"
  //"github.com/arcaneiceman/GoVector/govec"
)


type RemoteClock struct {
  address string
  conn *net.UDPConn
  Clock int

}
func (rclock *RemoteClock) Connect(address string) error {
  rclock.address = address

  addr,err := net.ResolveUDPAddr("udp", address)
  if err != nil {
    return err
  }

  conn,err := net.DialUDP("udp", nil, addr)
  if err != nil {
    return err
  }
  rclock.conn = conn
  return nil
}
func (rclock *RemoteClock) Disconnect() error {
  panic("Not Implemented")
}
func (rclock *RemoteClock) Read() int {
  ch := make(chan int)
  go func() {
    buf := make([]byte, 1024)
    _,err := rclock.conn.Write([]byte("GET"))  // request clock value
    if err != nil {
      ch <- -1
    }
    n,_,err := rclock.conn.ReadFromUDP(buf)  // block waiting for reply
    if err != nil {
      ch <- -1
    }
    command := string(buf[:2])  // GET, PUT
    payload := string(buf[5:n])  // <delta>
    switch command {
    case "PUT":
      clockVal,_ := strconv.Atoi(payload)
      ch <- clockVal // push clock value onto channel
    default:
      ch <- -1
    }
  }()
  timeout := make(chan bool, 1)
  go func() {
      time.Sleep(1 * time.Second)
      timeout <- true
  }()

  select {
  case <-ch:
    // channel got value before timeout
    rclock.Clock = <-ch
    return <- ch
  case <-timeout:
    // no response within timeout
    rclock.Clock = -1
    return -1
  }
}
func (rclock *RemoteClock) Sync(delta int) {
    //buf := make([]byte, 1024)
    rclock.conn.Write([]byte("PUT::" + string(delta)))  // send update to clock value
}




// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, err
  }
  defer file.Close()

  var lines []string
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    lines = append(lines, scanner.Text())
  }
  return lines, scanner.Err()
}





// Assume slavesfile is address for single slave for now
func master(address string, time int, threshold int, slavesfile string, logfile string) {
  // initialize connection
  addr,err := net.ResolveUDPAddr("udp", address)
  connMaster,err := net.DialUDP("udp", nil, addr)

  if err != nil {
    log.Fatal(err)
  }
  defer connMaster.Close()

  // initialize connection to slave
  slaveAddresses, err := readLines(slavesfile)//ioutil.ReadFile(slavesfile)
  if (err != nil) {
    log.Fatal(err)
  }
  remoteClocks := make([]*RemoteClock, len(slaveAddresses))
  for idx, slaveAddress := range slaveAddresses {
    remoteClocks[idx].Connect(slaveAddress)
  }


  // Initialization
  clocks := make([]int, len(slaveAddresses)+1)
  clocks[len(slaveAddresses)] = time


  //Logger := govec.Initialize("MyProcess", logfile)



  // Loop polling slaves
    for idx, slave := range remoteClocks {
      clocks[idx] = slave.Read()
    }


  // poll slaves for latest clock values

  // average the values + compute correction

  // push update to slaves


  panic("Not Implemented")
}


func slave(address string, time int, logfile string) {
  // initialize connection
  addr,err := net.ResolveUDPAddr("udp", address)
  conn,err := net.DialUDP("udp", nil, addr)

  if err != nil {
    log.Fatal(err)
  }
  defer conn.Close()

  // initialize logging
  //Logger := govec.Initialize("MyProcess", logfile)

  // initialize data structure
  clock := time

  // loop waiting for query from master
  for {
      // receive request from master
      buf := make([]byte, 1024)
      n,_,err := conn.ReadFromUDP(buf)

      // buf: GET or PUT::<delta>
      command := string(buf[:2])  // GET, PUT
      payload := string(buf[5:n])  // <delta>
      delta,err := strconv.Atoi(payload)
      var msg []byte
      switch command {
      case "GET":
        msg = []byte("PUT::" + string(clock))  // also need to send synchronization round
      case "PUT":
        clock = clock + delta
        msg = []byte("ACK::" + string(payload))
      default:
        msg = []byte("ERR::Unknown command.")
      }

      _,err = conn.Write(msg)
      if err != nil {
        // do something?
      }
  }

  panic("Not Implemented")
}


func main() {
  if len(os.Args) < 3 {
    log.Fatal("Invalid number of parameters specified.")
  }

  switch os.Args[1] {
  case "-m":
    address := ""//os.Args[2]
    time := 2
    threshold := 5
    slavesfile := ""
    logfile := os.Args[6]


    log.Println("Running in master mode with address %s", address)

    master(address, time, threshold, slavesfile, logfile)
  case "-s":
    address := os.Args[2]
    time := 1
    logfile := os.Args[4]
    log.Println("Running in slave mode with address %s", address)
    slave(address, time, logfile)
  default:
    log.Fatal("Invalid master/slave flag.")
  }
}
