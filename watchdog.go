package mqtt

import (
	"errors"
	"time"
)

// ackWatchdog monitors the packet sent to the broker that expects a response
// triggers a reconnection if no response is received within the timeout period.
func ackWatchdog(c *client, ackTimeout time.Duration) {
	defer c.workers.Done()

	ticker := time.NewTicker(ackTimeout / 2)
	defer ticker.Stop()

	DEBUG.Println(NET, "ack watchdog started")
	c.logger.Debug("ack watchdog started", componentAttr(NET))

	for {
		select {
		case <-c.stop:
			DEBUG.Println(NET, "ack watchdog stopping")
			c.logger.Debug("ack watchdog stopping", componentAttr(NET))
			return
		case <-ticker.C:
			fastReconnectTimeVal := c.fastReconnectCheckStartTime.Load().(time.Time)
			lastReceivedTimeVal := c.lastReceived.Load().(time.Time)

			if fastReconnectTimeVal.After(lastReceivedTimeVal) {
				if time.Since(fastReconnectTimeVal) >= ackTimeout {
					DEBUG.Println(NET, "ack watchdog timeout detected")
					c.logger.Debug("ack watchdog timeout detected", componentAttr(NET))
					c.internalConnLost(errors.New("ack watchdog timeout detected"))
					return
				}
			}
		}
	}
}
