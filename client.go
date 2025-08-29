/*
 * Copyright (c) 2021 IBM Corp and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 *    Matt Brittan
 */

// Portions copyright © 2018 TIBCO Software Inc.

// Package mqtt provides an MQTT v3.1.1 client library.
package mqtt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/gojek/paho.mqtt.golang/packets"
)

// Client is the interface definition for a Client as used by this
// library, the interface is primarily to allow mocking tests.
//
// It is an MQTT v3.1.1 client for communicating
// with an MQTT server using non-blocking methods that allow work
// to be done in the background.
// An application may connect to an MQTT server using:
//
//		A plain TCP socket (e.g. mqtt://test.mosquitto.org:1833)
//		A secure SSL/TLS socket (e.g. tls://test.mosquitto.org:8883)
//		A websocket (e.g ws://test.mosquitto.org:8080 or wss://test.mosquitto.org:8081)
//	 Something else (using `options.CustomOpenConnectionFn`)
//
// To enable ensured message delivery at Quality of Service (QoS) levels
// described in the MQTT spec, a message persistence mechanism must be
// used. This is done by providing a type which implements the Store
// interface. For convenience, FileStore and MemoryStore are provided
// implementations that should be sufficient for most use cases. More
// information can be found in their respective documentation.
// Numerous connection options may be specified by configuring a
// and then supplying a ClientOptions type.
// Implementations of Client must be safe for concurrent use by multiple
// goroutines
type Client interface {
	// IsConnected returns a bool signifying whether
	// the client is connected or not.
	IsConnected() bool
	// IsConnectionOpen return a bool signifying whether the client has an active
	// connection to mqtt broker, i.e not in disconnected or reconnect mode
	IsConnectionOpen() bool
	// Connect will create a connection to the message broker, by default
	// it will attempt to connect at v3.1.1 and auto retry at v3.1 if that
	// fails
	Connect() Token
	// Disconnect will end the connection with the server, but not before waiting
	// the specified number of milliseconds to wait for existing work to be
	// completed.
	Disconnect(quiesce uint)
	// Publish will publish a message with the specified QoS and content
	// to the specified topic.
	// Returns a token to track delivery of the message to the broker
	Publish(topic string, qos byte, retained bool, payload interface{}) Token
	// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler.
	//
	// If options.OrderMatters is true (the default) then callback must not block or
	// call functions within this package that may block (e.g. Publish) other than in
	// a new go routine.
	// callback must be safe for concurrent use by multiple goroutines.
	Subscribe(topic string, qos byte, callback MessageHandler) Token
	// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to
	// be executed when a message is published on one of the topics provided, or nil for the
	// default handler.
	//
	// If options.OrderMatters is true (the default) then callback must not block or
	// call functions within this package that may block (e.g. Publish) other than in
	// a new go routine.
	// callback must be safe for concurrent use by multiple goroutines.
	SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token
	// Unsubscribe will end the subscription from each of the topics provided.
	// Messages published to those topics from other clients will no longer be
	// received.
	Unsubscribe(topics ...string) Token
	// AddRoute allows you to add a handler for messages on a specific topic
	// without making a subscription. For example having a different handler
	// for parts of a wildcard subscription or for receiving retained messages
	// upon connection (before Sub scribe can be processed).
	//
	// If options.OrderMatters is true (the default) then callback must not block or
	// call functions within this package that may block (e.g. Publish) other than in
	// a new go routine.
	// callback must be safe for concurrent use by multiple goroutines.
	AddRoute(topic string, callback MessageHandler)
	// OptionsReader returns a ClientOptionsReader which is a copy of the clientoptions
	// in use by the client.
	OptionsReader() ClientOptionsReader
}

// client implements the Client interface
// clients are safe for concurrent use by multiple
// goroutines
type client struct {
	lastSent        atomic.Value // time.Time - the last time a packet was successfully sent to network
	lastReceived    atomic.Value // time.Time - the last time a packet was successfully received from network
	pingOutstanding int32        // set to 1 if a ping has been sent but response not ret received

	status connectionStatus // see constants in status.go for values

	messageIds // effectively a map from message id to token completor

	obound    chan *PacketAndToken // outgoing publish packet
	oboundP   chan *PacketAndToken // outgoing 'priority' packet (anything other than publish)
	msgRouter *router              // routes topics to handlers
	persist   Store
	options   ClientOptions
	optionsMu sync.Mutex // Protects the options in a few limited cases where needed for testing

	conn   net.Conn   // the network connection, must only be set with connMu locked (only used when starting/stopping workers)
	connMu sync.Mutex // mutex for the connection (again only used in two functions)

	stop         chan struct{}  // Closed to request that workers stop
	workers      sync.WaitGroup // used to wait for workers to complete (ping, keepalive, errwatch, resume)
	commsStopped chan struct{}  // closed when the comms routines have stopped (kept running until after workers have closed to avoid deadlocks)

	backoff *backoffController
	logger  *slog.Logger // logger for the client, set to options.Logger if not nil, otherwise uses slog.Default() logger
}

// NewClient will create an MQTT v3.1.1 client with all of the options specified
// in the provided ClientOptions. The client must have the Connect method called
// on it before it may be used. This is to make sure resources (such as a net
// connection) are created before the application is actually ready.
func NewClient(o *ClientOptions) Client {
	c := &client{}
	c.options = *o

	switch c.options.ProtocolVersion {
	case 3, 4:
		c.options.protocolVersionExplicit = true
	case 0x83, 0x84:
		c.options.protocolVersionExplicit = true
	default:
		c.options.ProtocolVersion = 4
		c.options.protocolVersionExplicit = false
	}
	if o.LogVerbosity == LogLevelDefault {
		c.logger = noopSLogger
	} else {
		c.logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: o.LogVerbosity.toSlogLevel(),
		})).With(slog.String("clientID", c.options.ClientID))
	}

	if c.options.Store == nil {
		c.options.Store = NewMemoryStore()
	}
	c.persist = c.options.Store
	c.messageIds = messageIds{index: make(map[uint16]tokenCompletor), logger: c.logger}
	c.msgRouter = newRouter(c.logger)
	c.msgRouter.setDefaultHandler(c.options.DefaultPublishHandler)
	c.obound = make(chan *PacketAndToken)
	c.oboundP = make(chan *PacketAndToken)
	c.backoff = newBackoffController()
	return c
}

// AddRoute allows you to add a handler for messages on a specific topic
// without making a subscription. For example having a different handler
// for parts of a wildcard subscription
//
// If options.OrderMatters is true (the default) then callback must not block or
// call functions within this package that may block (e.g. Publish) other than in
// a new go routine.
// callback must be safe for concurrent use by multiple goroutines.
func (c *client) AddRoute(topic string, callback MessageHandler) {
	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}
}

// IsConnected returns a bool signifying whether
// the client is connected or not.
// connected means that the connection is up now OR it will
// be established/reestablished automatically when possible
// Warning: The connection status may change at any time so use this with care!
func (c *client) IsConnected() bool {
	// This will need to change if additional statuses are added
	s, r := c.status.ConnectionStatusRetry()
	switch {
	case s == connected:
		return true
	case c.options.ConnectRetry && s == connecting:
		return true
	case c.options.AutoReconnect:
		return s == reconnecting || (s == disconnecting && r) // r indicates we will reconnect
	default:
		return false
	}
}

// IsConnectionOpen return a bool signifying whether the client has an active
// connection to mqtt broker, i.e. not in disconnected or reconnect mode
// Warning: The connection status may change at any time so use this with care!
func (c *client) IsConnectionOpen() bool {
	return c.status.ConnectionStatus() == connected
}

// ErrNotConnected is the error returned from function calls that are
// made when the client is not connected to a broker
var ErrNotConnected = errors.New("not Connected")

// Connect will create a connection to the message broker, by default
// it will attempt to connect at v3.1.1 and auto retry at v3.1 if that
// fails
// Note: If using QOS1+ and CleanSession=false it is advisable to add
// routes (or a DefaultPublishHandler) prior to calling Connect()
// because queued messages may be delivered immediately post connection
func (c *client) Connect() Token {
	t := newToken(packets.Connect).(*ConnectToken)
	DEBUG.Println(CLI, "Connect()")
	c.logger.Debug("Connect()", componentAttr(CLI))

	connectionUp, err := c.status.Connecting()
	if err != nil {
		if err == errAlreadyConnectedOrReconnecting && c.options.AutoReconnect {
			// When reconnection is active we don't consider calls tro Connect to ba an error (mainly for compatability)
			WARN.Println(CLI, "Connect() called but not disconnected")
			c.logger.Warn("Connect() called but not disconnected", componentAttr(CLI))

			t.returnCode = packets.Accepted
			t.flowComplete()
			return t
		}
		ERROR.Println(CLI, err) // CONNECT should never be called unless we are disconnected
		c.logger.Error("Connect() failed", slog.String("error", err.Error()), componentAttr(CLI))

		t.setError(err)
		return t
	}

	c.persist.Open()
	if c.options.ConnectRetry {
		c.reserveStoredPublishIDs() // Reserve IDs to allow publishing before connect complete
	}

	go func() {
		if len(c.options.Servers) == 0 {
			t.setError(fmt.Errorf("no servers defined to connect to"))
			if err := connectionUp(false); err != nil {
				ERROR.Println(CLI, err.Error())
				c.logger.Error(err.Error(), componentAttr(CLI))
			}
			return
		}

	RETRYCONN:
		var conn net.Conn
		var rc byte
		var err error
		conn, rc, t.sessionPresent, err = c.attemptConnection()
		if err != nil {
			if c.options.ConnectRetry {
				DEBUG.Println(CLI, "Connect failed, sleeping for", int(c.options.ConnectRetryInterval.Seconds()), "seconds and will then retry, error:", err.Error())
				c.logger.Debug("Connect failed, sleeping for retry_interval and will then retry",
					slog.Int("retry_interval_sec", int(c.options.ConnectRetryInterval.Seconds())),
					slog.String("error", err.Error()),
					componentAttr(CLI),
				)

				time.Sleep(c.options.ConnectRetryInterval)

				if c.status.ConnectionStatus() == connecting { // Possible connection aborted elsewhere
					goto RETRYCONN
				}
			}
			ERROR.Println(CLI, "Failed to connect to a broker")
			c.logger.Error("Failed to connect to a broker", slog.String("error", err.Error()), componentAttr(CLI))

			c.persist.Close()
			t.returnCode = rc
			t.setError(err)
			if err := connectionUp(false); err != nil {
				ERROR.Println(CLI, err.Error())
				c.logger.Error("Connect() failed", slog.String("error", err.Error()), componentAttr(CLI))
			}
			return
		}
		inboundFromStore := make(chan packets.ControlPacket)           // there may be some inbound comms packets in the store that are awaiting processing
		if c.startCommsWorkers(conn, connectionUp, inboundFromStore) { // note that this takes care of updating the status (to connected or disconnected)
			// Take care of any messages in the store
			if !c.options.CleanSession {
				c.resume(c.options.ResumeSubs, inboundFromStore)
			} else {
				c.persist.Reset()
			}
		} else { // Note: With the new status subsystem this should only happen if Disconnect called simultaneously with the above
			WARN.Println(CLI, "Connect() called but connection established in another goroutine")
			c.logger.Warn("Connect() called but connection established in another goroutine", componentAttr(CLI))
		}

		close(inboundFromStore)
		t.flowComplete()
		DEBUG.Println(CLI, "exit startClient")
		c.logger.Debug("exit startClient", componentAttr(CLI))
	}()
	return t
}

// internal function used to reconnect the client when it loses its connection
// The connection status MUST be reconnecting prior to calling this function (via call to status.connectionLost)
func (c *client) reconnect(connectionUp connCompletedFn) {
	DEBUG.Println(CLI, "enter reconnect")
	c.logger.Debug("enter reconnect", componentAttr(CLI))

	var (
		initSleep = 1 * time.Second
		conn      net.Conn
	)

	// If the reason of connection lost is same as the before one, sleep timer is set before attempting connection is started.
	// Sleep time is exponentially increased as the same situation continues
	if slp, isContinual := c.backoff.sleepWithBackoff("connectionLost", initSleep, c.options.MaxReconnectInterval, 3*time.Second, true); isContinual {
		DEBUG.Println(CLI, "Detect continual connection lost after reconnect, slept for", int(slp.Seconds()), "seconds")
		c.logger.Debug("Detect continual connection lost after reconnect, slept for", slog.Int("seconds", int(slp.Seconds())), componentAttr(CLI))
	}

	for {
		if nil != c.options.OnReconnecting {
			c.options.OnReconnecting(c, &c.options)
		}
		var err error
		conn, _, _, err = c.attemptConnection()
		if err == nil {
			break
		}
		sleep, _ := c.backoff.sleepWithBackoff("attemptReconnection", initSleep, c.options.MaxReconnectInterval, c.options.ConnectTimeout, false)
		DEBUG.Println(CLI, "Reconnect failed, slept for", int(sleep.Seconds()), "seconds:", err)
		c.logger.Debug("Reconnect failed, slept for", slog.Int("seconds", int(sleep.Seconds())), slog.String("error", err.Error()), componentAttr(CLI))

		if c.status.ConnectionStatus() != reconnecting { // Disconnect may have been called
			if err := connectionUp(false); err != nil { // Should always return an error
				ERROR.Println(CLI, err.Error())
				c.logger.Error(err.Error(), componentAttr(CLI))
			}
			DEBUG.Println(CLI, "Client moved to disconnected state while reconnecting, abandoning reconnect")
			c.logger.Debug("Client moved to disconnected state while reconnecting, abandoning reconnect", componentAttr(CLI))
			return
		}
	}

	inboundFromStore := make(chan packets.ControlPacket)           // there may be some inbound comms packets in the store that are awaiting processing
	if c.startCommsWorkers(conn, connectionUp, inboundFromStore) { // note that this takes care of updating the status (to connected or disconnected)
		c.resume(c.options.ResumeSubs, inboundFromStore)
	}
	close(inboundFromStore)
}

// attemptConnection makes a single attempt to connect to each of the brokers
// the protocol version to use is passed in (as c.options.ProtocolVersion)
// Note: Does not set c.conn in order to minimise race conditions
// Returns:
// net.Conn - Connected network connection
// byte - Return code (packets.Accepted indicates a successful connection).
// bool - SessionPresent flag from the connect ack (only valid if packets.Accepted)
// err - Error (err != nil guarantees that conn has been set to active connection).
func (c *client) attemptConnection() (net.Conn, byte, bool, error) {
	protocolVersion := c.options.ProtocolVersion
	var (
		sessionPresent bool
		conn           net.Conn
		err            error
		rc             byte
	)

	c.optionsMu.Lock() // Protect c.options.Servers so that servers can be added in test cases
	brokers := c.options.Servers
	c.optionsMu.Unlock()
	for _, broker := range brokers {
		cm := newConnectMsgFromOptions(&c.options, broker)
		DEBUG.Println(CLI, "about to write new connect msg")
		c.logger.Debug("about to write new connect msg", componentAttr(CLI))
	CONN:
		tlsCfg := c.options.TLSConfig
		if c.options.OnConnectAttempt != nil {
			DEBUG.Println(CLI, "using custom onConnectAttempt handler...")
			c.logger.Debug("using custom onConnectAttempt handler", componentAttr(CLI))

			tlsCfg = c.options.OnConnectAttempt(broker, c.options.TLSConfig)
		}
		connDeadline := time.Now().Add(c.options.ConnectTimeout) // Time by which connection must be established
		dialer := c.options.Dialer
		if dialer == nil { //
			WARN.Println(CLI, "dialer was nil, using default")
			c.logger.Warn("dialer was nil, using default", componentAttr(CLI))
			dialer = &net.Dialer{Timeout: 30 * time.Second}
		}
		// Start by opening the network connection (tcp, tls, ws) etc
		if c.options.CustomOpenConnectionFn != nil {
			conn, err = c.options.CustomOpenConnectionFn(broker, c.options)
		} else {
			conn, err = openConnection(broker, tlsCfg, c.options.ConnectTimeout, c.options.HTTPHeaders, c.options.WebsocketOptions, dialer)
		}
		if err != nil {
			ERROR.Println(CLI, err.Error())
			c.logger.Error("Failed to connect to broker", slog.String("error", err.Error()), componentAttr(CLI))
			WARN.Println(CLI, "failed to connect to broker, trying next")
			c.logger.Warn("failed to connect to broker, trying next", componentAttr(CLI))

			rc = packets.ErrNetworkError
			continue
		}
		DEBUG.Println(CLI, "socket connected to broker")
		c.logger.Debug("socket connected to broker", componentAttr(CLI))

		// Now we perform the MQTT connection handshake ensuring that it does not exceed the timeout
		if err := conn.SetDeadline(connDeadline); err != nil {
			ERROR.Println(CLI, "set deadline for handshake ", err)
			c.logger.Error("set deadline for handshake", slog.String("error", err.Error()), componentAttr(CLI))
		}

		// Now we perform the MQTT connection handshake
		rc, sessionPresent, err = connectMQTT(conn, cm, protocolVersion, c.logger)
		if rc == packets.Accepted {
			if err := conn.SetDeadline(time.Time{}); err != nil {
				ERROR.Println(CLI, "reset deadline following handshake ", err)
				c.logger.Error("reset deadline following handshake", slog.String("error", err.Error()), componentAttr(CLI))
			}
			break // successfully connected
		}

		// We may have to attempt the connection with MQTT 3.1
		_ = conn.Close()

		if !c.options.protocolVersionExplicit && protocolVersion == 4 { // try falling back to 3.1?
			DEBUG.Println(CLI, "Trying reconnect using MQTT 3.1 protocol")
			c.logger.Debug("Trying reconnect using MQTT 3.1 protocol", componentAttr(CLI))

			protocolVersion = 3
			goto CONN
		}
		if c.options.protocolVersionExplicit { // to maintain logging from previous version
			ERROR.Println(CLI, "Connecting to", broker, "CONNACK was not CONN_ACCEPTED, but rather", packets.ConnackReturnCodes[rc])
			c.logger.Error("CONNACK was not CONN_ACCEPTED, but rather",
				slog.String("CONNACK", packets.ConnackReturnCodes[rc]),
				slog.String("broker", broker.String()),
				componentAttr(CLI),
			)
		}
	}
	// If the connection was successful we set member variable and lock in the protocol version for future connection attempts (and users)
	if rc == packets.Accepted {
		c.options.ProtocolVersion = protocolVersion
		c.options.protocolVersionExplicit = true
	} else {
		// Maintain same error format as used previously
		if rc != packets.ErrNetworkError { // mqtt error
			err = packets.ConnErrors[rc]
		} else { // network error (if this occurred in ConnectMQTT then err will be nil)
			err = fmt.Errorf("%w : %w", packets.ConnErrors[rc], err)
		}
	}
	return conn, rc, sessionPresent, err
}

// Disconnect will end the connection with the server, but not before waiting
// the specified number of milliseconds to wait for existing work to be
// completed.
// WARNING: `Disconnect` may return before all activities (goroutines) have completed. This means that
// reusing the `client` may lead to panics. If you want to reconnect when the connection drops then use
// `SetAutoReconnect` and/or `SetConnectRetry`options instead of implementing this yourself.
func (c *client) Disconnect(quiesce uint) {
	done := make(chan struct{}) // Simplest way to ensure quiesce is always honoured
	go func() {
		defer close(done)
		disDone, err := c.status.Disconnecting()
		if err != nil {
			// Status has been set to disconnecting, but we had to wait for something else to complete
			WARN.Println(CLI, err.Error())
			c.logger.Warn("Disconnect() called but waiting for something else to complete", slog.String("error", err.Error()), componentAttr(CLI))
			return
		}
		defer func() {
			c.disconnect() // Force disconnection
			disDone()      // Update status
		}()
		DEBUG.Println(CLI, "disconnecting")
		c.logger.Debug("disconnecting", componentAttr(CLI))

		dm := packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
		dt := newToken(packets.Disconnect)
		select {
		case c.oboundP <- &PacketAndToken{p: dm, t: dt}:
			// wait for work to finish, or quiesce time consumed
			DEBUG.Println(CLI, "calling WaitTimeout")
			c.logger.Debug("calling WaitTimeout", componentAttr(CLI))

			dt.WaitTimeout(time.Duration(quiesce) * time.Millisecond)
			DEBUG.Println(CLI, "WaitTimeout done")
			c.logger.Debug("WaitTimeout done", componentAttr(CLI))
		// Below code causes a potential data race. Following status refactor it should no longer be required
		// but leaving in as need to check code further.
		// case <-c.commsStopped:
		//           WARN.Println("Disconnect packet could not be sent because comms stopped")
		case <-time.After(time.Duration(quiesce) * time.Millisecond):
			WARN.Println("Disconnect packet not sent due to timeout")
			c.logger.Warn("Disconnect packet not sent due to timeout", componentAttr(CLI))
		}
	}()

	// Return when done or after timeout expires (would like to change but this maintains compatibility)
	delay := time.NewTimer(time.Duration(quiesce) * time.Millisecond)
	select {
	case <-done:
		if !delay.Stop() {
			<-delay.C
		}
	case <-delay.C:
	}
}

// forceDisconnect will end the connection with the mqtt broker immediately (used for tests only)
func (c *client) forceDisconnect() {
	disDone, err := c.status.Disconnecting()
	if err != nil {
		// Possible that we are not actually connected
		WARN.Println(CLI, err.Error())
		c.logger.Warn("error when forcing disconnect", slog.String("error", err.Error()), componentAttr(CLI))
		return
	}
	DEBUG.Println(CLI, "forcefully disconnecting")
	c.logger.Debug("forcefully disconnecting", componentAttr(CLI))
	c.disconnect()
	disDone()
}

// disconnect cleans up after a final disconnection (user requested so no auto reconnection)
func (c *client) disconnect() {
	done := c.stopCommsWorkers()
	if done != nil {
		<-done // Wait until the disconnect is complete (to limit chance that another connection will be started)
		DEBUG.Println(CLI, "forcefully disconnecting")
		c.logger.Debug("forcefully disconnecting", componentAttr(CLI))
		c.messageIds.cleanUp()
		DEBUG.Println(CLI, "disconnected")
		c.logger.Debug("disconnected", componentAttr(CLI))
		c.persist.Close()
	}
}

// internalConnLost cleanup when connection is lost or an error occurs
// Note: This function will not block
func (c *client) internalConnLost(whyConnLost error) {
	// It is possible that internalConnLost will be called multiple times simultaneously
	// (including after sending a DisconnectPacket) as such we only do cleanup etc if the
	// routines were actually running and are not being disconnected at users request
	DEBUG.Println(CLI, "internalConnLost called")
	c.logger.Debug("internalConnLost called", componentAttr(CLI))
	disDone, err := c.status.ConnectionLost(c.options.AutoReconnect && c.status.ConnectionStatus() > connecting)
	if err != nil {
		if err == errConnLossWhileDisconnecting || err == errAlreadyHandlingConnectionLoss {
			return // Loss of connection is expected or already being handled
		}
		ERROR.Println(CLI, fmt.Sprintf("internalConnLost unexpected status: %s", err.Error()))
		c.logger.Error("internalConnLost unexpected status", slog.String("error", err.Error()), componentAttr(CLI))
		return
	}

	// c.stopCommsWorker returns a channel that is closed when the operation completes. This was required prior
	// to the implementation of proper status management but has been left in place, for now, to minimise change
	stopDone := c.stopCommsWorkers()
	// stopDone was required in previous versions because there was no connectionLost status (and there were
	// issues with status handling). This code has been left in place for the time being just in case the new
	// status handling contains bugs (refactoring required at some point).
	if stopDone == nil { // stopDone will be nil if workers already in the process of stopping or stopped
		ERROR.Println(CLI, "internalConnLost stopDone unexpectedly nil - BUG BUG")
		c.logger.Error("internalConnLost stopDone unexpectedly nil - BUG BUG", componentAttr(CLI))
		// Cannot really do anything other than leave things disconnected
		if _, err = disDone(false); err != nil { // Safest option - cannot leave status as connectionLost
			ERROR.Println(CLI, fmt.Sprintf("internalConnLost failed to set status to disconnected (stopDone): %s", err.Error()))
			c.logger.Error("internalConnLost failed to set status to disconnected (stopDone)", slog.String("error", err.Error()), componentAttr(CLI))
		}
		return
	}

	// It may take a while for the disconnection to complete whatever called us needs to exit cleanly so finnish in goRoutine
	go func() {
		DEBUG.Println(CLI, "internalConnLost waiting on workers")
		c.logger.Debug("internalConnLost waiting on workers", componentAttr(CLI))
		<-stopDone
		DEBUG.Println(CLI, "internalConnLost workers stopped")
		c.logger.Debug("internalConnLost workers stopped", componentAttr(CLI))

		reConnDone, err := disDone(true)
		if err != nil {
			ERROR.Println(CLI, "failure whilst reporting completion of disconnect", err)
			c.logger.Error("failure whilst reporting completion of disconnect", slog.Any("error", err), componentAttr(CLI))
		} else if reConnDone == nil { // Should never happen
			ERROR.Println(CLI, "BUG BUG BUG reconnection function is nil", err)
			c.logger.Error("BUG BUG BUG reconnection function is nil", slog.Any("error", err), componentAttr(CLI))
		}

		reconnect := err == nil && reConnDone != nil

		if c.options.CleanSession && !reconnect {
			c.messageIds.cleanUp() // completes PUB/SUB/UNSUB tokens
		} else if !c.options.ResumeSubs {
			c.messageIds.cleanUpSubscribe() // completes SUB/UNSUB tokens
		}
		if reconnect {
			go c.reconnect(reConnDone) // Will set connection status to reconnecting
		}
		if c.options.OnConnectionLost != nil {
			go c.options.OnConnectionLost(c, whyConnLost)
		}
		DEBUG.Println(CLI, "internalConnLost complete")
		c.logger.Debug("internalConnLost complete", componentAttr(CLI))
	}()
}

// startCommsWorkers is called when the connection is up.
// It starts off the routines needed to process incoming and outgoing messages.
// Returns true if the comms workers were started (i.e. successful connection)
// connectionUp(true) will be called once everything is up;  connectionUp(false) will be called on failure
func (c *client) startCommsWorkers(conn net.Conn, connectionUp connCompletedFn, inboundFromStore <-chan packets.ControlPacket) bool {
	DEBUG.Println(CLI, "startCommsWorkers called")
	c.logger.Debug("startCommsWorkers called", componentAttr(CLI))
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil { // Should never happen due to new status handling; leaving in for safety for the time being
		WARN.Println(CLI, "startCommsWorkers called when commsworkers already running BUG BUG")
		c.logger.Warn("startCommsWorkers called when commsworkers already running BUG BUG", componentAttr(CLI))
		_ = conn.Close() // No use for the new network connection
		if err := connectionUp(false); err != nil {
			ERROR.Println(CLI, err.Error())
			c.logger.Error("startCommsWorkers failed", slog.String("error", err.Error()), componentAttr(CLI))
		}
		return false
	}
	c.conn = conn // Store the connection

	c.stop = make(chan struct{})
	if c.options.KeepAlive != 0 {
		atomic.StoreInt32(&c.pingOutstanding, 0)
		c.lastReceived.Store(time.Now())
		c.lastSent.Store(time.Now())
		c.workers.Add(1)
		go keepalive(c, conn)
	}

	// matchAndDispatch will process messages received from the network. It may generate acknowledgements
	// It will complete when incomingPubChan is closed and will close ackOut prior to exiting
	incomingPubChan := make(chan *packets.PublishPacket)
	c.workers.Add(1) // Done will be called when ackOut is closed
	ackOut := c.msgRouter.matchAndDispatch(incomingPubChan, c.options.Order, c)

	// The connection is now ready for use (we spin up a few go routines below).
	// It is possible that Disconnect has been called in the interim...
	// issue 675：we will allow the connection to complete before the Disconnect is allowed to proceed
	//   as if a Disconnect event occurred immediately after connectionUp(true) completed.
	if err := connectionUp(true); err != nil {
		ERROR.Println(CLI, err)
		c.logger.Error(err.Error(), componentAttr(CLI))
	}

	DEBUG.Println(CLI, "client is connected/reconnected")
	c.logger.Debug("client is connected/reconnected", componentAttr(CLI))
	if c.options.OnConnect != nil {
		go c.options.OnConnect(c)
	}

	// c.oboundP and c.obound need to stay active for the life of the client because, depending upon the options,
	// messages may be published while the client is disconnected (they will block unless in a goroutine). However
	// to keep the comms routines clean we want to shutdown the input messages it uses so create out own channels
	// and copy data across.
	commsobound := make(chan *PacketAndToken)  // outgoing publish packets
	commsoboundP := make(chan *PacketAndToken) // outgoing 'priority' packet
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		for {
			select {
			case msg := <-c.oboundP:
				commsoboundP <- msg
			case msg := <-c.obound:
				commsobound <- msg
			case msg, ok := <-ackOut:
				if !ok {
					ackOut = nil     // ignore channel going forward
					c.workers.Done() // matchAndDispatch has completed
					continue         // await next message
				}
				commsoboundP <- msg
			case <-c.stop:
				// Attempt to transmit any outstanding acknowledgements (this may well fail but should work if this is a clean disconnect)
				if ackOut != nil {
					for msg := range ackOut {
						commsoboundP <- msg
					}
					c.workers.Done() // matchAndDispatch has completed
				}
				close(commsoboundP) // Nothing sending to these channels anymore so close them and allow comms routines to exit
				close(commsobound)
				DEBUG.Println(CLI, "startCommsWorkers output redirector finished")
				c.logger.Debug("startCommsWorkers output redirector finished", componentAttr(CLI))
				return
			}
		}
	}()

	commsIncomingPub, commsErrors := startComms(c.conn, c, inboundFromStore, commsoboundP, commsobound, c.logger)
	c.commsStopped = make(chan struct{})
	go func() {
		for {
			if commsIncomingPub == nil && commsErrors == nil {
				break
			}
			select {
			case pub, ok := <-commsIncomingPub:
				if !ok {
					// Incoming comms has shutdown
					close(incomingPubChan) // stop the router
					commsIncomingPub = nil
					continue
				}
				// Care is needed here because an error elsewhere could trigger a deadlock
			sendPubLoop:
				for {
					select {
					case incomingPubChan <- pub:
						break sendPubLoop
					case err, ok := <-commsErrors:
						if !ok { // commsErrors has been closed so we can ignore it
							commsErrors = nil
							continue
						}
						ERROR.Println(CLI, "Connect comms goroutine - error triggered during send Pub", err)
						c.logger.Error("Connect comms goroutine - error triggered during send Pub", slog.String("error", err.Error()), componentAttr(CLI))
						c.internalConnLost(err) // no harm in calling this if the connection is already down (or shutdown is in progress)
						continue
					}
				}
			case err, ok := <-commsErrors:
				if !ok {
					commsErrors = nil
					continue
				}
				ERROR.Println(CLI, "Connect comms goroutine - error triggered", err)
				c.logger.Error("Connect comms goroutine - error triggered", err.Error(), componentAttr(CLI))
				c.internalConnLost(err) // no harm in calling this if the connection is already down (or shutdown is in progress)
				continue
			}
		}
		DEBUG.Println(CLI, "incoming comms goroutine done")
		c.logger.Debug("incoming comms goroutine done", componentAttr(CLI))
		close(c.commsStopped)
	}()
	DEBUG.Println(CLI, "startCommsWorkers done")
	c.logger.Debug("startCommsWorkers done", componentAttr(CLI))
	return true
}

// stopWorkersAndComms - Cleanly shuts down worker go routines (including the comms routines) and waits until everything has stopped
// Returns nil if workers did not need to be stopped; otherwise returns a channel which will be closed when the stop is complete
// Note: This may block so run as a go routine if calling from any of the comms routines
// Note2: It should be possible to simplify this now that the new status management code is in place.
func (c *client) stopCommsWorkers() chan struct{} {
	DEBUG.Println(CLI, "stopCommsWorkers called")
	c.logger.Debug("stopCommsWorkers called", componentAttr(CLI))
	// It is possible that this function will be called multiple times simultaneously due to the way things get shutdown
	c.connMu.Lock()
	if c.conn == nil {
		DEBUG.Println(CLI, "stopCommsWorkers done (not running)")
		c.logger.Debug("stopCommsWorkers done (not running)", componentAttr(CLI))
		c.connMu.Unlock()
		return nil
	}

	// It is important that everything is stopped in the correct order to avoid deadlocks. The main issue here is
	// the router because it both receives incoming publish messages and also sends outgoing acknowledgements. To
	// avoid issues we signal the workers to stop and close the connection (it is probably already closed but
	// there is no harm in being sure). We can then wait for the workers to finnish before closing outbound comms
	// channels which will allow the comms routines to exit.

	// We stop all non-comms related workers first (ping, keepalive, errwatch, resume etc) so they don't get blocked waiting on comms
	close(c.stop)     // Signal for workers to stop
	c.conn.Close()    // Possible that this is already closed but no harm in closing again
	c.conn = nil      // Important that this is the only place that this is set to nil
	c.connMu.Unlock() // As the connection is now nil we can unlock the mu (allowing subsequent calls to exit immediately)

	doneChan := make(chan struct{})

	go func() {
		DEBUG.Println(CLI, "stopCommsWorkers waiting for workers")
		c.logger.Debug("stopCommsWorkers waiting for workers", componentAttr(CLI))
		c.workers.Wait()

		// Stopping the workers will allow the comms routines to exit; we wait for these to complete
		DEBUG.Println(CLI, "stopCommsWorkers waiting for comms")
		c.logger.Debug("stopCommsWorkers waiting for comms", componentAttr(CLI))
		<-c.commsStopped // wait for comms routine to stop

		DEBUG.Println(CLI, "stopCommsWorkers done")
		c.logger.Debug("stopCommsWorkers done", componentAttr(CLI))
		close(doneChan)
	}()
	return doneChan
}

// Publish will publish a message with the specified QoS and content
// to the specified topic.
// Returns a token to track delivery of the message to the broker
func (c *client) Publish(topic string, qos byte, retained bool, payload interface{}) Token {
	token := newToken(packets.Publish).(*PublishToken)
	DEBUG.Println(CLI, "enter Publish")
	c.logger.Debug("enter Publish", componentAttr(CLI))
	switch {
	case !c.IsConnected():
		token.setError(ErrNotConnected)
		return token
	case c.status.ConnectionStatus() == reconnecting && qos == 0:
		// message written to store and will be sent when connection comes up
		token.flowComplete()
		return token
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = qos
	pub.TopicName = topic
	pub.Retain = retained
	switch p := payload.(type) {
	case string:
		pub.Payload = []byte(p)
	case []byte:
		pub.Payload = p
	case bytes.Buffer:
		pub.Payload = p.Bytes()
	default:
		token.setError(fmt.Errorf("unknown payload type"))
		return token
	}

	if pub.Qos != 0 && pub.MessageID == 0 {
		mID := c.getID(token)
		if mID == 0 {
			token.setError(fmt.Errorf("no message IDs available"))
			return token
		}
		pub.MessageID = mID
		token.messageID = mID
	}
	persistOutbound(c.persist, pub, c.logger)
	switch c.status.ConnectionStatus() {
	case connecting:
		DEBUG.Println(CLI, "storing publish message (connecting), topic:", topic)
		c.logger.Debug("storing publish message (connecting)", slog.String("topic", topic), componentAttr(CLI))
	case reconnecting:
		DEBUG.Println(CLI, "storing publish message (reconnecting), topic:", topic)
		c.logger.Debug("storing publish message (reconnecting)", slog.String("topic", topic), componentAttr(CLI))
	case disconnecting:
		DEBUG.Println(CLI, "storing publish message (disconnecting), topic:", topic)
		c.logger.Debug("storing publish message (disconnecting)", slog.String("topic", topic), componentAttr(CLI))
	default:
		DEBUG.Println(CLI, "sending publish message, topic:", topic)
		c.logger.Debug("sending publish message", slog.String("topic", topic), componentAttr(CLI))
		publishWaitTimeout := c.options.WriteTimeout
		if publishWaitTimeout == 0 {
			publishWaitTimeout = time.Second * 30
		}

		t := time.NewTimer(publishWaitTimeout)
		defer t.Stop()

		select {
		case c.obound <- &PacketAndToken{p: pub, t: token}:
		case <-t.C:
			token.setError(errors.New("publish was broken by timeout"))
		}
	}
	return token
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
//
// If options.OrderMatters is true (the default) then callback must not block or
// call functions within this package that may block (e.g. Publish) other than in
// a new go routine.
// callback must be safe for concurrent use by multiple goroutines.
func (c *client) Subscribe(topic string, qos byte, callback MessageHandler) Token {
	token := newToken(packets.Subscribe).(*SubscribeToken)
	DEBUG.Println(CLI, "enter Subscribe")
	c.logger.Debug("enter Subscribe", componentAttr(CLI))
	if !c.IsConnected() {
		token.setError(ErrNotConnected)
		return token
	}
	if !c.IsConnectionOpen() {
		switch {
		case !c.options.ResumeSubs:
			// if not connected and resumeSubs not set this sub will be thrown away
			token.setError(fmt.Errorf("not currently connected and ResumeSubs not set"))
			return token
		case c.options.CleanSession && c.status.ConnectionStatus() == reconnecting:
			// if reconnecting and cleanSession is true this sub will be thrown away
			token.setError(fmt.Errorf("reconnecting state and cleansession is true"))
			return token
		}
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if err := validateTopicAndQos(topic, qos); err != nil {
		token.setError(err)
		return token
	}
	sub.Topics = append(sub.Topics, topic)
	sub.Qoss = append(sub.Qoss, qos)

	if strings.HasPrefix(topic, "$share/") {
		topic = strings.Join(strings.Split(topic, "/")[2:], "/")
	}

	if strings.HasPrefix(topic, "$queue/") {
		topic = strings.TrimPrefix(topic, "$queue/")
	}

	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}

	token.subs = append(token.subs, topic)

	if sub.MessageID == 0 {
		mID := c.getID(token)
		if mID == 0 {
			token.setError(fmt.Errorf("no message IDs available"))
			return token
		}
		sub.MessageID = mID
		token.messageID = mID
	}
	DEBUG.Println(CLI, sub.String())
	c.logger.Debug("subscribe packet", slog.String("packet", sub.String()), componentAttr(CLI))

	if c.options.ResumeSubs { // Only persist if we need this to resume subs after a disconnection
		persistOutbound(c.persist, sub, c.logger)
	}
	switch c.status.ConnectionStatus() {
	case connecting:
		DEBUG.Println(CLI, "storing subscribe message (connecting), topic:", topic)
		c.logger.Debug("storing subscribe message (connecting)", slog.String("topic", topic), componentAttr(CLI))
	case reconnecting:
		DEBUG.Println(CLI, "storing subscribe message (reconnecting), topic:", topic)
		c.logger.Debug("storing subscribe message (reconnecting)", slog.String("topic", topic), componentAttr(CLI))
	case disconnecting:
		DEBUG.Println(CLI, "storing subscribe message (disconnecting), topic:", topic)
		c.logger.Debug("storing subscribe message (disconnecting)", slog.String("topic", topic), componentAttr(CLI))
	default:
		DEBUG.Println(CLI, "sending subscribe message, topic:", topic)
		c.logger.Debug("sending subscribe message", slog.String("topic", topic), componentAttr(CLI))
		subscribeWaitTimeout := c.options.WriteTimeout
		if subscribeWaitTimeout == 0 {
			subscribeWaitTimeout = time.Second * 30
		}
		select {
		case c.oboundP <- &PacketAndToken{p: sub, t: token}:
		case <-time.After(subscribeWaitTimeout):
			token.setError(errors.New("subscribe was broken by timeout"))
		}
	}
	DEBUG.Println(CLI, "exit Subscribe")
	c.logger.Debug("exit Subscribe", componentAttr(CLI))
	return token
}

// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to
// be executed when a message is published on one of the topics provided.
//
// If options.OrderMatters is true (the default) then callback must not block or
// call functions within this package that may block (e.g. Publish) other than in
// a new go routine.
// callback must be safe for concurrent use by multiple goroutines.
func (c *client) SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token {
	var err error
	token := newToken(packets.Subscribe).(*SubscribeToken)
	DEBUG.Println(CLI, "enter SubscribeMultiple")
	c.logger.Debug("enter SubscribeMultiple", componentAttr(CLI))
	if !c.IsConnected() {
		token.setError(ErrNotConnected)
		return token
	}
	if !c.IsConnectionOpen() {
		switch {
		case !c.options.ResumeSubs:
			// if not connected and resumesubs not set this sub will be thrown away
			token.setError(fmt.Errorf("not currently connected and ResumeSubs not set"))
			return token
		case c.options.CleanSession && c.status.ConnectionStatus() == reconnecting:
			// if reconnecting and cleanSession is true this sub will be thrown away
			token.setError(fmt.Errorf("reconnecting state and cleansession is true"))
			return token
		}
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if sub.Topics, sub.Qoss, err = validateSubscribeMap(filters); err != nil {
		token.setError(err)
		return token
	}

	if callback != nil {
		for topic := range filters {
			c.msgRouter.addRoute(topic, callback)
		}
	}
	token.subs = make([]string, len(sub.Topics))
	copy(token.subs, sub.Topics)

	if sub.MessageID == 0 {
		mID := c.getID(token)
		if mID == 0 {
			token.setError(fmt.Errorf("no message IDs available"))
			return token
		}
		sub.MessageID = mID
		token.messageID = mID
	}
	if c.options.ResumeSubs { // Only persist if we need this to resume subs after a disconnection
		persistOutbound(c.persist, sub, c.logger)
	}
	switch c.status.ConnectionStatus() {
	case connecting:
		DEBUG.Println(CLI, "storing subscribe message (connecting), topics:", sub.Topics)
		c.logger.Debug("storing subscribe message (connecting)", slog.String("topics", strings.Join(sub.Topics, ",")), componentAttr(CLI))
	case reconnecting:
		DEBUG.Println(CLI, "storing subscribe message (reconnecting), topics:", sub.Topics)
		c.logger.Debug("storing subscribe message (reconnecting)", slog.String("topics", strings.Join(sub.Topics, ",")), componentAttr(CLI))
	case disconnecting:
		DEBUG.Println(CLI, "storing subscribe message (disconnecting), topics:", sub.Topics)
		c.logger.Debug("storing subscribe message (disconnecting)", slog.String("topics", strings.Join(sub.Topics, ",")), componentAttr(CLI))
	default:
		DEBUG.Println(CLI, "sending subscribe message, topics:", sub.Topics)
		c.logger.Debug("sending subscribe message", slog.String("topics", strings.Join(sub.Topics, ",")), componentAttr(CLI))
		subscribeWaitTimeout := c.options.WriteTimeout
		if subscribeWaitTimeout == 0 {
			subscribeWaitTimeout = time.Second * 30
		}
		select {
		case c.oboundP <- &PacketAndToken{p: sub, t: token}:
		case <-time.After(subscribeWaitTimeout):
			token.setError(errors.New("subscribe was broken by timeout"))
		}
	}
	DEBUG.Println(CLI, "exit SubscribeMultiple")
	c.logger.Debug("exit SubscribeMultiple", componentAttr(CLI))
	return token
}

// reserveStoredPublishIDs reserves the ids for publish packets in the persistent store to ensure these are not duplicated
func (c *client) reserveStoredPublishIDs() {
	// The resume function sets the stored id for publish packets only (some other packets
	// will get new ids in net code). This means that the only keys we need to ensure are
	// unique are the publish ones (and these will completed/replaced in resume() )
	if !c.options.CleanSession {
		storedKeys := c.persist.All()
		for _, key := range storedKeys {
			packet := c.persist.Get(key)
			if packet == nil {
				continue
			}
			switch packet.(type) {
			case *packets.PublishPacket:
				details := packet.Details()
				token := &PlaceHolderToken{id: details.MessageID}
				c.claimID(token, details.MessageID)
			}
		}
	}
}

// Load all stored messages and resend them
// Call this to ensure QOS > 1,2 even after an application crash
// Note: This function will exit if c.stop is closed (this allows the shutdown to proceed avoiding a potential deadlock)
// other than that it does not return until all messages in the store have been sent (connect() does not complete its
// token before this completes)
func (c *client) resume(subscription bool, ibound chan packets.ControlPacket) {
	DEBUG.Println(STR, "enter Resume")
	c.logger.Debug("enter Resume", componentAttr(STR))

	// Prior to sending a message getSemaphore will be called and once sent releaseSemaphore will be called
	// with the token (so semaphore can be released when ACK received if applicable).
	// Using a weighted semaphore rather than channels because this retains ordering
	getSemaphore := func() {}                    // Default = do nothing
	releaseSemaphore := func(_ *PublishToken) {} // Default = do nothing
	var sem *semaphore.Weighted
	if c.options.MaxResumePubInFlight > 0 {
		sem = semaphore.NewWeighted(int64(c.options.MaxResumePubInFlight))
		ctx, cancel := context.WithCancel(context.Background()) // Context needed for semaphore
		defer cancel()                                          // ensure context gets cancelled

		go func() {
			select {
			case <-c.stop: // Request to stop (due to comm error etc)
				cancel()
			case <-ctx.Done(): // resume completed normally
			}
		}()

		getSemaphore = func() { sem.Acquire(ctx, 1) }
		releaseSemaphore = func(token *PublishToken) { // Note: If token never completes then resume() may stall (will still exit on ctx.Done())
			go func() {
				select {
				case <-token.Done():
				case <-ctx.Done():
				}
				sem.Release(1)
			}()
		}
	}

	storedKeys := c.persist.All()
	for _, key := range storedKeys {
		packet := c.persist.Get(key)
		if packet == nil {
			DEBUG.Println(STR, fmt.Sprintf("resume found NIL packet (%s)", key))
			c.logger.Debug(fmt.Sprintf("resume found NIL packet (%s)", key), componentAttr(STR))
			continue
		}
		details := packet.Details()
		if isKeyOutbound(key) {
			switch p := packet.(type) {
			case *packets.SubscribePacket:
				if subscription {
					DEBUG.Println(STR, fmt.Sprintf("loaded pending subscribe (%d)", details.MessageID))
					c.logger.Debug(fmt.Sprintf("loaded pending subscribe (%d)", details.MessageID), componentAttr(STR))
					subPacket := packet.(*packets.SubscribePacket)
					token := newToken(packets.Subscribe).(*SubscribeToken)
					token.messageID = details.MessageID
					token.subs = append(token.subs, subPacket.Topics...)
					c.claimID(token, details.MessageID)
					select {
					case c.oboundP <- &PacketAndToken{p: packet, t: token}:
					case <-c.stop:
						DEBUG.Println(STR, "resume exiting due to stop")
						c.logger.Debug("resume exiting due to stop", componentAttr(STR))
						return
					}
				} else {
					c.persist.Del(key) // Unsubscribe packets should not be retained following a reconnect
				}
			case *packets.UnsubscribePacket:
				if subscription {
					DEBUG.Println(STR, fmt.Sprintf("loaded pending unsubscribe (%d)", details.MessageID))
					c.logger.Debug(fmt.Sprintf("loaded pending unsubscribe (%d)", details.MessageID), componentAttr(STR))
					token := newToken(packets.Unsubscribe).(*UnsubscribeToken)
					select {
					case c.oboundP <- &PacketAndToken{p: packet, t: token}:
					case <-c.stop:
						DEBUG.Println(STR, "resume exiting due to stop")
						c.logger.Debug("resume exiting due to stop", componentAttr(STR))
						return
					}
				} else {
					c.persist.Del(key) // Unsubscribe packets should not be retained following a reconnect
				}
			case *packets.PubrelPacket:
				DEBUG.Println(STR, fmt.Sprintf("loaded pending pubrel (%d)", details.MessageID))
				c.logger.Debug(fmt.Sprintf("loaded pending pubrel (%d)", details.MessageID), componentAttr(STR))
				select {
				case c.oboundP <- &PacketAndToken{p: packet, t: nil}:
				case <-c.stop:
					DEBUG.Println(STR, "resume exiting due to stop")
					c.logger.Debug("resume exiting due to stop", componentAttr(STR))
					return
				}
			case *packets.PublishPacket:
				// spec: If the DUP flag is set to 0, it indicates that this is the first occasion that the Client or
				// Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is set to 1, it indicates that
				// this might be re-delivery of an earlier attempt to send the Packet.
				//
				// If the message is in the store than an attempt at delivery has been made (note that the message may
				// never have made it onto the wire but tracking that would be complicated!).
				if p.Qos != 0 { // spec: The DUP flag MUST be set to 0 for all QoS 0 messages
					p.Dup = true
				}
				token := newToken(packets.Publish).(*PublishToken)
				token.messageID = details.MessageID
				c.claimID(token, details.MessageID)
				DEBUG.Println(STR, fmt.Sprintf("loaded pending publish (%d)", details.MessageID))
				c.logger.Debug(fmt.Sprintf("loaded pending publish (%d)", details.MessageID), componentAttr(STR))
				DEBUG.Println(STR, details)
				c.logger.Debug("details", slog.String("messageID", fmt.Sprintf("%d", details.MessageID)), slog.Int("QoS", int(details.Qos)), componentAttr(STR))
				getSemaphore()
				select {
				case c.obound <- &PacketAndToken{p: p, t: token}:
				case <-c.stop:
					DEBUG.Println(STR, "resume exiting due to stop")
					c.logger.Debug("resume exiting due to stop", componentAttr(STR))
					return
				}
				releaseSemaphore(token) // If limiting simultaneous messages then we need to know when message is acknowledged
			default:
				ERROR.Println(STR, fmt.Sprintf("invalid message type (inbound - %T) in store (discarded)", packet))
				c.logger.Error("invalid message type in store (discarded)",
					slog.String("type", fmt.Sprintf("%T", packet)),
					componentAttr(STR),
				)
				c.persist.Del(key)
			}
		} else {
			switch packet.(type) {
			case *packets.PubrelPacket:
				DEBUG.Println(STR, fmt.Sprintf("loaded pending incomming (%d)", details.MessageID))
				c.logger.Debug("loaded pending incomming", slog.String("messageID", fmt.Sprintf("%d", details.MessageID)), componentAttr(STR))
				select {
				case ibound <- packet:
				case <-c.stop:
					DEBUG.Println(STR, "resume exiting due to stop (ibound <- packet)")
					c.logger.Debug("resume exiting due to stop (ibound <- packet)", componentAttr(STR))
					return
				}
			default:
				ERROR.Println(STR, fmt.Sprintf("invalid message type (%T) in store (discarded)", packet))
				c.logger.Error("invalid message type in store (discarded)",
					slog.String("type", fmt.Sprintf("%T", packet)),
					componentAttr(STR),
				)
				c.persist.Del(key)
			}
		}
	}
	DEBUG.Println(STR, "exit resume")
	c.logger.Debug("exit resume", componentAttr(STR))
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (c *client) Unsubscribe(topics ...string) Token {
	token := newToken(packets.Unsubscribe).(*UnsubscribeToken)
	DEBUG.Println(CLI, "enter Unsubscribe")
	c.logger.Debug("enter Unsubscribe", componentAttr(CLI))
	if !c.IsConnected() {
		token.setError(ErrNotConnected)
		return token
	}
	if !c.IsConnectionOpen() {
		switch {
		case !c.options.ResumeSubs:
			// if not connected and resumeSubs not set this unsub will be thrown away
			token.setError(fmt.Errorf("not currently connected and ResumeSubs not set"))
			return token
		case c.options.CleanSession && c.status.ConnectionStatus() == reconnecting:
			// if reconnecting and cleanSession is true this unsub will be thrown away
			token.setError(fmt.Errorf("reconnecting state and cleansession is true"))
			return token
		}
	}
	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	unsub.Topics = make([]string, len(topics))
	copy(unsub.Topics, topics)

	if unsub.MessageID == 0 {
		mID := c.getID(token)
		if mID == 0 {
			token.setError(fmt.Errorf("no message IDs available"))
			return token
		}
		unsub.MessageID = mID
		token.messageID = mID
	}

	if c.options.ResumeSubs { // Only persist if we need this to resume subs after a disconnection
		persistOutbound(c.persist, unsub, c.logger)
	}

	switch c.status.ConnectionStatus() {
	case connecting:
		DEBUG.Println(CLI, "storing unsubscribe message (connecting), topics:", topics)
		c.logger.Debug("storing unsubscribe message (connecting)", slog.String("topics", strings.Join(topics, ",")), componentAttr(CLI))
	case reconnecting:
		DEBUG.Println(CLI, "storing unsubscribe message (reconnecting), topics:", topics)
		c.logger.Debug("storing unsubscribe message (reconnecting)", slog.String("topics", strings.Join(topics, ",")), componentAttr(CLI))
	case disconnecting:
		DEBUG.Println(CLI, "storing unsubscribe message (reconnecting), topics:", topics)
		c.logger.Debug("storing unsubscribe message (disconnecting)", slog.String("topics", strings.Join(topics, ",")), componentAttr(CLI))
	default:
		DEBUG.Println(CLI, "sending unsubscribe message, topics:", topics)
		c.logger.Debug("sending unsubscribe message", slog.String("topics", strings.Join(topics, ",")), componentAttr(CLI))
		subscribeWaitTimeout := c.options.WriteTimeout
		if subscribeWaitTimeout == 0 {
			subscribeWaitTimeout = time.Second * 30
		}
		select {
		case c.oboundP <- &PacketAndToken{p: unsub, t: token}:
			for _, topic := range topics {
				c.msgRouter.deleteRoute(topic)
			}
		case <-time.After(subscribeWaitTimeout):
			token.setError(errors.New("unsubscribe was broken by timeout"))
		}
	}

	DEBUG.Println(CLI, "exit Unsubscribe")
	c.logger.Debug("exit Unsubscribe", componentAttr(CLI))
	return token
}

// OptionsReader returns a ClientOptionsReader which is a copy of the clientoptions
// in use by the client.
func (c *client) OptionsReader() ClientOptionsReader {
	r := ClientOptionsReader{options: &c.options}
	return r
}

// DefaultConnectionLostHandler is a definition of a function that simply
// reports to the DEBUG log the reason for the client losing a connection.
func DefaultConnectionLostHandler(client Client, reason error) {
	DEBUG.Println("Connection lost:", reason.Error())
}

// UpdateLastReceived - Will be called whenever a packet is received off the network
// This is used by the keepalive routine to
func (c *client) UpdateLastReceived() {
	if c.options.KeepAlive != 0 {
		c.lastReceived.Store(time.Now())
	}
}

// UpdateLastReceived - Will be called whenever a packet is successfully transmitted to the network
func (c *client) UpdateLastSent() {
	if c.options.KeepAlive != 0 {
		c.lastSent.Store(time.Now())
	}
}

// getWriteTimeOut returns the writetimeout (duration to wait when writing to the connection) or 0 if none
func (c *client) getWriteTimeOut() time.Duration {
	return c.options.WriteTimeout
}

// persistOutbound adds the packet to the outbound store
func (c *client) persistOutbound(m packets.ControlPacket) {
	persistOutbound(c.persist, m, c.logger)
}

// persistInbound adds the packet to the inbound store
func (c *client) persistInbound(m packets.ControlPacket) {
	persistInbound(c.persist, m, c.logger)
}

// pingRespReceived will be called by the network routines when a ping response is received
func (c *client) pingRespReceived() {
	atomic.StoreInt32(&c.pingOutstanding, 0)
}
