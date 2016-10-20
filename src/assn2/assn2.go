package main

import (
  "time"
  "fmt"
  "strconv"
  "os"
  "net"
  "log"
  "github.com/arcaneiceman/GoVector/govec"
  "bufio"

)

type Request struct {
  Verb string
  Payload int
}



/****** HELPER FUNCTIONS ******/
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








/********* SLAVE **********/

type Clock struct {
  Time int
}
func(this *Clock) GetTime() int {
  return this.Time
}
func(this *Clock) SetTime(time int) {
  this.Time = time
}
func(this *Clock) Correct(offset int) {
    this.Time += offset
}
func(this *Clock) ToString() string {
  return strconv.Itoa(this.Time)
}


func slave(address string, initTime int, logfile string) {
  // Setup connection to listen for incoming UDP packets
  addr,err := net.ResolveUDPAddr("udp", address)
  //conn,err := net.DialUDP("udp", nil, addr)
  conn,err := net.ListenUDP("udp", addr)

  if err != nil {
    log.Fatal(err)
  }
  defer conn.Close()

  // initialize logging
  Logger := govec.Initialize("Slave process", logfile)

  clock := Clock{initTime}
  fmt.Println("Set clock to", clock.ToString())

   clockTicker := time.NewTicker(time.Millisecond * 500)
   defer clockTicker.Stop()

  // thread to update slave's local clock
  go func() {
    for {
      select {
      case <- clockTicker.C:
        oldTime := clock.GetTime()
        clock.SetTime(oldTime + 1)
        fmt.Println("Clock updated to ", clock.ToString())
      }
    }
  }()

  // loop waiting for query from master
  for {
    buf := make([]byte, 1024)
    var inRequest Request
    _,raddr,_ := conn.ReadFromUDP(buf)  // block waiting for reply

    Logger.UnpackReceive("Receiving message", buf, &inRequest)

    switch inRequest.Verb {
    case "GET":
      fmt.Println("Master requested my clock value")
      sendClock := Logger.PrepareSend("Sending Message", clock)
      conn.WriteToUDP(sendClock,raddr)
      fmt.Println("I sent Master my clock value")
    case "PUT":
      delta := inRequest.Payload
      clock.Correct(delta)
    default:
      log.Fatal("Slave received invalid request from master.")
    }

  }
}

/******** END SLAVE **********/

func master(address string, initTime int, threshold int, slavesfile string, logfile string) {
  addr,_ := net.ResolveUDPAddr("udp", address)
  conn,err := net.DialUDP("udp", nil, addr)
  if err != nil {
    log.Fatal("Couldn't connect")
  }
  Logger := govec.Initialize("Slave process", logfile)

  slaveAddresses, err := readLines(slavesfile)
  if (err != nil) {
    log.Fatal(err)
  }
  clock := Clock{initTime}
  //clocks := make([]int, len(slaveAddresses)+1)


  clockTicker := time.NewTicker(time.Millisecond * 500)
  defer clockTicker.Stop()

   // thread to update slave's local clock
   go func() {
     for {
       select {
       case <- clockTicker.C:
         oldTime := clock.GetTime()
         clock.SetTime(oldTime + 1)
         //fmt.Println("Clock updated to ", clock.ToString())
       }
     }
   }()

   pollTicker := time.NewTicker(3 * time.Second)
   defer pollTicker.Stop()

   // thread to poll slaves and compute delta
   for {
     select {
     case <- pollTicker.C:
       fmt.Println("Starting sync round")
       slaveCh := make(chan int)
       for i,slaveAddress := range slaveAddresses {
         go func(slaveAddress string) {
           fmt.Printf("\tGetting clock vaule for slave %v at %v\n",i,slaveAddress)


           outRequest := Request{"GET",1000}
           finalSend := Logger.PrepareSend("Sending Message", outRequest)
           //conn.WriteToUDP(finalSend, addr)
           conn.Write(finalSend)
           //Rcv clock
           buf := make([]byte, 1024)
           var slaveClock Clock
           conn.ReadFromUDP(buf)  // block waiting for reply
           Logger.UnpackReceive("Receiving message", buf, &slaveClock)


           slaveCh <- slaveClock.GetTime()
         }(slaveAddress)
         go func() {
             time.Sleep(1 * time.Second)
             slaveCh <- -1
         }()
       }
       timesReceived := 1
       timeSum := 0 //clocks[len(slaveAddresses)]  // master clock
       select {
       case val := <-slaveCh:
         fmt.Printf("A slave returned %v\n",val)
         if (val >= 0) {
           timeSum += val
         }
         timesReceived++

         if (timesReceived >= len(slaveAddresses)) {
           timeSum += clock.GetTime()  // include the master clock
           delta := int(timeSum*1.0/timesReceived)
           fmt.Println("Do the avg")
           fmt.Printf("Total %v, num received %v, delta %v\n",timeSum,timesReceived,delta)

           clock.Correct(delta)
            for i,slaveAddress := range slaveAddresses {
              go func() {
                fmt.Printf("\tSetting clock vaule for slave %v at %v\n",i,slaveAddress)

                outRequest := Request{"PUT",delta}
                finalSend := Logger.PrepareSend("Sending Message", outRequest)
                conn.Write(finalSend)
              }()
           }
         }
       }
     }
   }







/* Working !!!!
   for {
     select {
     case <- pollTicker.C:
       fmt.Println("Starting sync round")
       slaveCh := make(chan int)
       for i,slaveAddress := range slaveAddresses {
         go func(slaveAddress string) {
           fmt.Printf("\tGetting clock vaule for slave %v at %v\n",i,slaveAddress)
           time.Sleep(1 * time.Second)
           slaveCh <- 1234
         }(slaveAddress)
         go func() {
             time.Sleep(1 * time.Second)
             slaveCh <- -1
         }()
       }
       select {
       case val := <-slaveCh:
         fmt.Printf("A slave returned %v\n",val)
       }
     }
   }
*/














   /*
   for idx,slaveAddress := range slaveAddresses {
     slaveCh := make(chan *Clock)
     go func(slaveAddress string) {
       //addr,_ := net.ResolveUDPAddr("udp", slaveAddress)
       ch := make(chan Clock)
       go func() {
         // make request of clients
         outRequest := Request{"GET",1000}
         finalSend := Logger.PrepareSend("Sending Message", outRequest)
         //conn.WriteToUDP(finalSend, addr)
         conn.Write(finalSend)
         //Rcv clock
         buf := make([]byte, 1024)
         var slaveClock Clock
         conn.ReadFromUDP(buf)  // block waiting for reply
         Logger.UnpackReceive("Receiving message", buf, &slaveClock)

         fmt.Println("Slave clock value", slaveClock)

         ch <- slaveClock
       }()
       timeout := make(chan bool, 1)
       go func() {
           time.Sleep(1 * time.Second)
           timeout <- true
       }()

       // handle response or timeout
       select {
       case <-ch:
         // channel got value before timeout
         fmt.Println("Got value before timeout")
         //return 1//<-ch
       case <-timeout:
         // no response within timeout
         fmt.Println("Timeout!")
         //return nil
       }
     }(slaveAddress)
     clocks[idx] = <-slaveCh
   }
   // compute avg value
   // delta := math.Avg(clocks)
   //delta := 1234

   // send update to clocks

//    clock.Correct(delta);  // fix the server time
//    for slaveAddress,idx := range slaveAddresses {
//      go func() {
//        outRequest := Request{"PUT",delta}
//        finalSend := Logger.PrepareSend("Sending Message", outRequest)
//        conn.WriteToUDP(finalSend, addr)
//      }()
//  }
*/

///////////////////////////////////////////////////////////////////































/*
    outRequest := Request{"PUT",1000}
    finalSend := Logger.PrepareSend("Sending Message", outRequest)
    conn.Write(finalSend)

    outRequest = Request{"GET",1000}
    finalSend = Logger.PrepareSend("Sending Message", outRequest)
    conn.Write(finalSend)


     //Rcv clock
     buf := make([]byte, 1024)
     var slaveClock Clock
     conn.ReadFromUDP(buf)  // block waiting for reply
     Logger.UnpackReceive("Receiving message", buf, &slaveClock)
     fmt.Println("Slave clock is set to", slaveClock)
*/

/*
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
*/















}



func main() {
  if len(os.Args) < 3 {
    log.Fatal("Invalid number of parameters specified.")
  }

  switch os.Args[1] {
  case "-m":
    address := os.Args[2]
    time := 2
    threshold := 5
    slavesfile := "slaves"
    logfile := os.Args[4]//os.Args[6]


    log.Println("Running in master mode with address %s", address)

    master(address, time, threshold, slavesfile, logfile)
/*
  address := os.Args[2]

  logfile := os.Args[4]
  log.Println("Running in master mode with address ", address)
    // Testing slave
    addr,_ := net.ResolveUDPAddr("udp", address)
    conn,err := net.DialUDP("udp", nil, addr)  // slave address
    //conn,err := net.ListenUDP("udp", addr) // slave address

    if err != nil {
      log.Fatal("Couldn't connect")
    }
    //conn.Write([]byte("GET"))
     fmt.Println("Message should be sent")
     Logger := govec.Initialize("Slave process", logfile)

     outRequest := Request{"PUT",1000}
     finalSend := Logger.PrepareSend("Sending Message", outRequest)
     conn.Write(finalSend)

     outRequest = Request{"GET",1000}
     finalSend = Logger.PrepareSend("Sending Message", outRequest)
     conn.Write(finalSend)


      //Rcv clock
      buf := make([]byte, 1024)
      var slaveClock Clock
      conn.ReadFromUDP(buf)  // block waiting for reply
      Logger.UnpackReceive("Receiving message", buf, &slaveClock)
      fmt.Println("Slave clock is set to", slaveClock)
*/

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
