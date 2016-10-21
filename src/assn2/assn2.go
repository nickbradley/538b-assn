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
  "math"
)

type Request struct {
  Verb string
  Payload int
}

type RemoteClock struct {
  Delta int
  Addr *net.UDPAddr
}


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



/****** HELPER FUNCTIONS ******/
func checkError(err error) {
  if (err != nil) {
    log.Fatal(err)
  }
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


func getAvg(a []int, threshold int) int {
  sum := 0
  count := 1
  b := a

  for i,e := range a {
    for j,f := range b {
      if (i != j && math.Abs(float64(e-f)) <= float64(threshold)) {
        //fmt.Printf("Adding %v to sum %v\n",e,sum)
        sum += e
        count++
        break
      }
    }
  }

  avg := -1
  //fmt.Println("Number for average %v", count)
  if count > 0 {
    //fmt.Printf("Dividing %v by %v\n",sum,count)
    avg = int(float64(sum) / float64(count))

  }
  return avg

}


/********* SLAVE **********/
func slave(address string, initTime int, logfile string) {
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
        //fmt.Println("Clock updated to ", clock.ToString())
      }
    }
  }()


  // Setup connection to listen for incoming UDP packets
  addr,err := net.ResolveUDPAddr("udp", address)
  checkError(err)
  conn,err := net.ListenUDP("udp", addr)
  checkError(err)
  defer conn.Close()




  // loop waiting for query from master
  for {
    buf := make([]byte, 1024)
    var inRequest Request
    _,masterAddr,_ := conn.ReadFromUDP(buf)  // block waiting for reply
    Logger.UnpackReceive("Received request from master", buf, &inRequest)

    switch inRequest.Verb {
    case "GET":
      fmt.Printf("%v<M> requested clock value from %v<S>...",masterAddr,addr)
      sendClock := Logger.PrepareSend("Sending clock value", clock)
      conn.WriteToUDP(sendClock,masterAddr)
      fmt.Printf("sent value %v.\n",clock.ToString())
    case "PUT":
      clockOffset := inRequest.Payload
      clock.Correct(clockOffset)
      fmt.Printf("%v<M> adjusted clock on %v<S> by %v. New clock value is %v.\n",masterAddr,addr,clockOffset,clock.ToString())
    default:
      log.Fatal("Received invalid request.")
    }

  }
}  // slave


/********* MASTER **********/
func master(address string, initTime int, threshold int, slavesfile string, logfile string) {
  // initialize logging
  Logger := govec.Initialize("Master Process", logfile)

  // read slave addresses
  slaveAddresses, err := readLines(slavesfile)
  checkError(err)

  // setup master's clock and increment every second
  clock := Clock{initTime}
  clockTicker := time.NewTicker(1 * time.Second)
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


   // Setup connection to listen for incoming UDP packets
   addr,err := net.ResolveUDPAddr("udp", address)
   checkError(err)
   conn,err := net.ListenUDP("udp", addr)
   checkError(err)
   defer conn.Close()



   // poll slaves every 3 seconds
   pollTicker := time.NewTicker(5 * time.Second)
   defer pollTicker.Stop()

   // thread to poll slaves and compute delta
   syncRound := 0
   for {
     select {
     case <- pollTicker.C:
       syncRound++
       fmt.Println("Starting sync round",syncRound)
       ch := make(chan RemoteClock)
       //masterTime := clock.GetTime()
       for i,slaveAddress := range slaveAddresses {
         go func(i int, slaveAddress string) {
           fmt.Printf("  Getting clock vaule for slave %v at %v\n",i,slaveAddress)
           addr,err := net.ResolveUDPAddr("udp", slaveAddress)
           checkError(err)

           outRequest := Request{"GET",0}
           finalSend := Logger.PrepareSend("Sending request for clock value", outRequest)
           conn.WriteToUDP(finalSend, addr)

           //Rcv clock
           buf := make([]byte, 1024)
           var slaveClock Clock
           conn.SetReadDeadline(time.Now().Add(1 * time.Second))
           _,_,err = conn.ReadFromUDP(buf)  // block waiting for reply
           if (err == nil) {
             Logger.UnpackReceive("Received clock value", buf, &slaveClock)



             //delta := slaveClock.GetTime() - masterTime
             delta := slaveClock.GetTime() - clock.GetTime()

             fmt.Printf("  Slave clock for %v is %v. Delta is %v-%v=%v.\n",addr, slaveClock.GetTime(), slaveClock.GetTime(),clock.GetTime(),delta)
             ch <- RemoteClock{delta, addr}
           }
         }(i, slaveAddress)
       }

       go func() {
         timeout := make(chan bool)
         go func() {
             time.Sleep(1 * time.Second)
             timeout <- true
         }()

         timesReceived := 0
         remoteClocks := make([]RemoteClock, len(slaveAddresses))
         L: for {
           select {
           case val := <-ch:
             //fmt.Printf("A slave returned %v\n",val)
             remoteClocks[timesReceived] = val
             timesReceived++

             //fmt.Printf("Times rvcd %v of %v\n",timesReceived,len(slaveAddresses))
             if (timesReceived >= len(slaveAddresses)){
               //fmt.Println("All slaves repsonded.")
               break L
             }
           case <-timeout:
             //fmt.Println("Reached timeout.", val)
             break L
           }
         }

         // Derive the deltas, compute avg and adjust the master and slave clocks
         deltas := make([]int, timesReceived)

         for i,rclock := range remoteClocks[0:timesReceived] {
           deltas[i] = rclock.Delta
         }

         avg := getAvg(deltas, threshold)

         clock.Correct(avg)  // adjust the master's clock
         fmt.Printf("Adjusting master by %v. New clock value is %v.\n",avg,clock.GetTime())

         for _,rclock := range remoteClocks[0:timesReceived] {
           fmt.Printf("Adjusting %v by %v\n",rclock.Addr,avg-rclock.Delta)
           go func(rclock RemoteClock) {
              outRequest := Request{"PUT",avg-rclock.Delta}
              finalSend := Logger.PrepareSend("Sending adjust value", outRequest)
              //fmt.Printf("Sending request to %v for sync round %v.\n",rclock.Addr, syncRound)
              conn.WriteToUDP(finalSend, rclock.Addr)
            }(rclock)
         }
       }()
     }
   }
}  // master



func main() {
  if len(os.Args) < 3 {
    log.Fatal("Invalid number of parameters specified.")
  }

  switch os.Args[1] {
  case "-m":
    address := os.Args[2]
    time,_ := strconv.Atoi(os.Args[3])
    threshold,_ := strconv.Atoi(os.Args[4])
    slavesfile := os.Args[5]
    logfile := os.Args[6]
    log.Printf("Running in master mode with address %v\n", address)
    master(address, time, threshold, slavesfile, logfile)
  case "-s":
    address := os.Args[2]
    time,_ := strconv.Atoi(os.Args[3])
    logfile := os.Args[4]
    log.Printf("Running in slave mode with address %v\n", address)
    slave(address, time, logfile)
  default:
    log.Fatal("Invalid master/slave flag.")
  }
}  // main
