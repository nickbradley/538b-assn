package main

import (
//  "time"
  "fmt"
  "strconv"
  "os"
  "net"
  "log"
  "github.com/arcaneiceman/GoVector/govec"
)

type Request struct {
  Verb string
  Payload int
}


/****** HELPER FUNCTIONS ******/
// readLines reads a whole file into memory
// and returns a slice of its lines.
/*
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
*/







/********* SLAVE **********/

type Clock struct {
  time int
}
func(this *Clock) GetTime() int {
  return this.time
}
func(this *Clock) SetTime(time int) {
  this.time = time
}
func(this *Clock) Correct(delta int) {
    this.time =+ delta
}
func(this *Clock) ToString() string {
  return strconv.Itoa(this.time)
}


func slave(address string, initTime int, logfile string) {
  // Setup connection to listen for incoming UDP packets
  addr,err := net.ResolveUDPAddr("udp", address)
  conn,err := net.ListenUDP("udp", addr)

  if err != nil {
    log.Fatal(err)
  }
  defer conn.Close()

  // initialize logging
  Logger := govec.Initialize("Slave process", logfile)

  clock := Clock{initTime}
  fmt.Println("Set clock to", clock.ToString())

//  ticker := time.NewTicker(time.Millisecond * 500)
//  defer ticker.Stop()

  // thread to update slave's local clock
  // go func() {
  //   for t := range ticker.C {
  //     oldTime := clock.GetTime()
  //     clock.SetTime(oldTime + 1)
  //     //fmt.Println("Clock updated to ", clock.ToString())
  //     //fmt.Println(t)
  //   }
  // }()

  // loop waiting for query from master
  for {
    fmt.Println("Waiting for msg")
    // block waiting for request
    buf := make([]byte, 1024)
    var inRequest Request
    conn.ReadFromUDP(buf)  // block waiting for reply
    fmt.Println("Msg received")

    Logger.UnpackReceive("Receiving message", buf, &inRequest)

    switch inRequest.Verb {
    case "GET":
      //send clock
      fmt.Println("GOT REQUEST")
    case "PUT":
      delta := inRequest.Payload
      clock.Correct(delta)
    default:
      log.Fatal("Slave received invalid request from master.")
    }

  }
}

/******** END SLAVE **********/

func master(address string, time int, threshold int, slavesfile string, logfile string) {

}



func main() {
  if len(os.Args) < 3 {
    log.Fatal("Invalid number of parameters specified.")
  }

  switch os.Args[1] {
  case "-m":
  //   address := ""//os.Args[2]
  //   time := 2
  //   threshold := 5
  //   slavesfile := ""
  //   logfile := os.Args[6]
  //
  //
  //   log.Println("Running in master mode with address %s", address)
  //
  //   master(address, time, threshold, slavesfile, logfile)

  address := os.Args[2]

  logfile := os.Args[4]
  log.Println("Running in master mode with address ", address)
    // Testing slave
    addr,_ := net.ResolveUDPAddr("udp", address)
    conn,err := net.DialUDP("udp", nil, addr)

    if err != nil {
      log.Fatal("Couldn't connect")
    }
    //conn.Write([]byte("GET"))
    fmt.Println("Message should be sent")
     Logger := govec.Initialize("Slave process", logfile)
     outRequest := Request{"GET",0}
     finalSend := Logger.PrepareSend("Sending Message", outRequest)
     conn.Write(finalSend)

  case "-s":
    address := os.Args[2]
    time := 1
    logfile := os.Args[4]
    log.Println("Running in slave mode with address ", address)
    slave(address, time, logfile)



  default:
    log.Fatal("Invalid master/slave flag.")
  }



}
