# zabbix-sender

Golang package, implement zabbix sender protocol for send metrics to zabbix.

Example:

```go
package main

import (
    "fmt"
    "time"

    "github.com/spetr/go-zabbix-sender"
)

const (
    defaultHost  = "localhost"
    defaultPort = "10051"
    agentActive = true
    trapper     = false
)

func main() {
    var metrics []*zabbix.Metric
    metrics = append(metrics, zabbix.NewMetric("localhost", "cpu", "1.22", agentActive, time.Now().Unix()))
    metrics = append(metrics, zabbix.NewMetric("localhost", "status", "OK", agentActive))
    metrics = append(metrics, zabbix.NewMetric("localhost", "someTrapper", "3.14", trapper))

    // Send metrics to zabbix
    z := zabbix.NewSender(defaultHost, defaultPort)
    resActive, errActive, resTrapper, errTrapper := z.SendMetrics(metrics)

    fmt.Printf("Agent active, response=%s, info=%s, error=%v\n", resActive.Response,resActive.Info, errActive)
    fmt.Printf("Trapper, response=%s, info=%s,error=%v\n", resTrapper.Response, resTrapper.Info,errTrapper)
}
```
