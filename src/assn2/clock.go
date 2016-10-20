package main

import (
  "time"
  "fmt"
  "strconv"
)

type Clock struct {
  time int
}
func(this *Clock) GetTime() int {
  return this.time
}
func(this *Clock) SetTime(time int) {
  this.time = time
}
func(this *Clock) ToString() string {
  return strconv.Itoa(this.time)
}


func main() {
  initTime := 1

  clock := Clock{initTime}
  //clock.setTime(initTime)
  clockVal := clock.ToString()
  fmt.Println("Set clock to " + clockVal)


  ticker := time.NewTicker(time.Millisecond * 500)
  go func() {
      for t := range ticker.C {
        oldTime := clock.GetTime()
        clock.SetTime(oldTime + 1)
        fmt.Println("Slave clock set to ", clock.GetTime())
          fmt.Println("Tick at", t)
      }
  }()
  time.Sleep(time.Millisecond * 1500)
  defer ticker.Stop()
  // fmt.Println("Ticker stopped")
}
