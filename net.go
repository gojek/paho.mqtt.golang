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

package mqtt

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gojek/paho.mqtt.golang/packets"
)

const closedNetConnErrorText = "use of closed network connection" // error string for closed conn (https://golang.org/src/net/error_test.go)

// ConnectMQTT takes a connected net.Conn and performs the initial MQTT handshake. Parameters are:
// conn - Connected net.Conn
// cm - Connect Packet with everything other than the protocol name/version populated (historical reasons)
// protocolVersion - The protocol version to attempt to connect with
//
// Note that, for backward compatibility, ConnectMQTT() suppresses the actual connection error (compare to connectMQTT()).
func ConnectMQTT(conn net.Conn, cm *packets.ConnectPacket, protocolVersion uint) (byte, bool) {
	logger := noopSLogger
	rc, sessionPresent, _ := connectMQTT(conn, cm, protocolVersion, logger)
	return rc, sessionPresent
}

func ConnectMQTTEx(conn net.Conn, cm *packets.ConnectPacket, protocolVersion uint, logger *slog.Logger) (byte, bool) {
	if logger == nil {
		logger = noopSLogger
	}
	rc, sessionPresent, _ := connectMQTT(conn, cm, protocolVersion, logger)
	return rc, sessionPresent
}

func connectMQTT(conn io.ReadWriter, cm *packets.ConnectPacket, protocolVersion uint, logger *slog.Logger) (byte, bool, error) {
	switch protocolVersion {
	case 3:
		DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
		logger.Debug("Using MQTT 3.1 protocol", componentAttr(CLI))
		cm.ProtocolName = "MQIsdp"
		cm.ProtocolVersion = 3
	case 0x83:
		DEBUG.Println(CLI, "Using MQTT 3.1b protocol")
		logger.Debug("Using MQTT 3.1b protocol", componentAttr(CLI))
		cm.ProtocolName = "MQIsdp"
		cm.ProtocolVersion = 0x83
	case 0x84:
		DEBUG.Println(CLI, "Using MQTT 3.1.1b protocol")
		logger.Debug("Using MQTT 3.1.1b protocol", componentAttr(CLI))
		cm.ProtocolName = "MQTT"
		cm.ProtocolVersion = 0x84
	default:
		DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
		logger.Debug("Using MQTT 3.1.1 protocol", componentAttr(CLI))
		cm.ProtocolName = "MQTT"
		cm.ProtocolVersion = 4
	}

	if err := cm.Write(conn); err != nil {
		ERROR.Println(CLI, err)
		logger.Error("connectMQTT write error", slog.String("error", err.Error()), componentAttr(CLI))
		return packets.ErrNetworkError, false, err
	}

	rc, sessionPresent, err := verifyCONNACK(conn, logger)
	return rc, sessionPresent, err
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func verifyCONNACK(conn io.Reader, logger *slog.Logger) (byte, bool, error) {
	DEBUG.Println(NET, "connect started")
	logger.Debug("connect started", componentAttr(NET))

	ca, err := packets.ReadPacket(conn)
	if err != nil {
		ERROR.Println(NET, "connect got error", err)
		logger.Error("connect got error", slog.String("error", err.Error()), componentAttr(NET))
		return packets.ErrNetworkError, false, err
	}

	if ca == nil {
		ERROR.Println(NET, "received nil packet")
		logger.Error("received nil packet", componentAttr(NET))
		return packets.ErrNetworkError, false, errors.New("nil CONNACK packet")
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		ERROR.Println(NET, "received msg that was not CONNACK")
		logger.Error("received msg that was not CONNACK", componentAttr(NET))
		return packets.ErrNetworkError, false, errors.New("non-CONNACK first packet received")
	}

	DEBUG.Println(NET, "received connack")
	logger.Debug("received connack", componentAttr(NET))
	return msg.ReturnCode, msg.SessionPresent, nil
}

// inbound encapsulates the output from startIncoming.
// err  - If != nil then an error has occurred
// cp - A control packet received over the network link
type inbound struct {
	err error
	cp  packets.ControlPacket
}

// startIncoming initiates a goroutine that reads incoming messages off the wire and sends them to the channel (returned).
// If there are any issues with the network connection then the returned channel will be closed and the goroutine will exit
// (so closing the connection will terminate the goroutine)
func startIncoming(conn io.Reader, logger *slog.Logger) <-chan inbound {
	var err error
	var cp packets.ControlPacket
	ibound := make(chan inbound)

	DEBUG.Println(NET, "incoming started")
	logger.Debug("incoming started", componentAttr(NET))

	go func() {
		for {
			if cp, err = packets.ReadPacket(conn); err != nil {
				// We do not want to log the error if it is due to the network connection having been closed
				// elsewhere (i.e. after sending DisconnectPacket). Detecting this situation is the subject of
				// https://github.com/golang/go/issues/4373
				if !strings.Contains(err.Error(), closedNetConnErrorText) {
					ibound <- inbound{err: err}
				}
				close(ibound)
				DEBUG.Println(NET, "incoming complete")
				logger.Debug("incoming complete", componentAttr(NET))
				return
			}
			DEBUG.Println(NET, "startIncoming Received Message")
			logger.Debug("startIncoming Received Message", componentAttr(NET))
			ibound <- inbound{cp: cp}
		}
	}()

	return ibound
}

// incomingComms encapsulates the possible output of the incomingComms routine. If err != nil then an error has occurred and
// the routine will have terminated; otherwise one of the other members should be non-nil
type incomingComms struct {
	err         error                  // If non-nil then there has been an error (ignore everything else)
	outbound    *PacketAndToken        // Packet (with token) than needs to be sent out (e.g. an acknowledgement)
	incomingPub *packets.PublishPacket // A new publish has been received; this will need to be passed on to our user
}

// startIncomingComms initiates incoming communications; this includes starting a goroutine to process incoming
// messages.
// Accepts a channel of inbound messages from the store (persisted messages); note this must be closed as soon as
// everything in the store has been sent.
// Returns a channel that will be passed any received packets; this will be closed on a network error (and inboundFromStore closed)
func startIncomingComms(conn io.Reader,
	c commsFns,
	inboundFromStore <-chan packets.ControlPacket,
	logger *slog.Logger,
) <-chan incomingComms {
	ibound := startIncoming(conn, logger) // Start goroutine that reads from network connection
	output := make(chan incomingComms)

	DEBUG.Println(NET, "startIncomingComms started")
	logger.Debug("startIncomingComms started", componentAttr(NET))
	go func() {
		for {
			if inboundFromStore == nil && ibound == nil {
				close(output)
				DEBUG.Println(NET, "startIncomingComms goroutine complete")
				logger.Debug("startIncomingComms goroutine complete", componentAttr(NET))
				return // As soon as ibound is closed we can exit (should have already processed an error)
			}
			DEBUG.Println(NET, "logic waiting for msg on ibound")
			logger.Debug("startIncomingComms logic waiting for msg on ibound", componentAttr(NET))

			var msg packets.ControlPacket
			var ok bool
			select {
			case msg, ok = <-inboundFromStore:
				if !ok {
					DEBUG.Println(NET, "startIncomingComms: inboundFromStore complete")
					logger.Debug("startIncomingComms: inboundFromStore complete", componentAttr(NET))
					inboundFromStore = nil // should happen quickly as this is only for persisted messages
					continue
				}
				DEBUG.Println(NET, "startIncomingComms: got msg from store")
				logger.Debug("startIncomingComms: got msg from store", componentAttr(NET))
			case ibMsg, ok := <-ibound:
				if !ok {
					DEBUG.Println(NET, "startIncomingComms: ibound complete")
					logger.Debug("startIncomingComms: ibound complete", componentAttr(NET))
					ibound = nil
					continue
				}
				DEBUG.Println(NET, "startIncomingComms: got msg on ibound")
				logger.Debug("startIncomingComms: got msg on ibound", componentAttr(NET))
				// If the inbound comms routine encounters any issues it will send us an error.
				if ibMsg.err != nil {
					output <- incomingComms{err: ibMsg.err}
					continue // Usually the channel will be closed immediately after sending an error but safer that we do not assume this
				}
				msg = ibMsg.cp

				c.persistInbound(msg)
				c.UpdateLastReceived() // Notify keepalive logic that we recently received a packet
			}

			switch m := msg.(type) {
			case *packets.PingrespPacket:
				DEBUG.Println(NET, "startIncomingComms: received pingresp")
				logger.Debug("startIncomingComms: received pingresp", componentAttr(NET))
				c.pingRespReceived()
			case *packets.SubackPacket:
				DEBUG.Println(NET, "startIncomingComms: received suback, id:", m.MessageID)
				logger.Debug("startIncomingComms: received suback", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				token := c.getToken(m.MessageID)

				if t, ok := token.(*SubscribeToken); ok {
					DEBUG.Println(NET, "startIncomingComms: granted qoss", m.ReturnCodes)
					logger.Debug("startIncomingComms: granted qoss", slog.Any("returnCodes", m.ReturnCodes), componentAttr(NET))
					for i, qos := range m.ReturnCodes {
						t.subResult[t.subs[i]] = qos
					}
				}

				token.flowComplete()
				c.freeID(m.MessageID)
			case *packets.UnsubackPacket:
				DEBUG.Println(NET, "startIncomingComms: received unsuback, id:", m.MessageID)
				logger.Debug("startIncomingComms: received unsuback", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				c.getToken(m.MessageID).flowComplete()
				c.freeID(m.MessageID)
			case *packets.PublishPacket:
				DEBUG.Println(NET, "startIncomingComms: received publish, msgId:", m.MessageID)
				logger.Debug("startIncomingComms: received publish", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				output <- incomingComms{incomingPub: m}
			case *packets.PubackPacket:
				DEBUG.Println(NET, "startIncomingComms: received puback, id:", m.MessageID)
				logger.Debug("startIncomingComms: received puback", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				c.getToken(m.MessageID).flowComplete()
				c.freeID(m.MessageID)
			case *packets.PubrecPacket:
				DEBUG.Println(NET, "startIncomingComms: received pubrec, id:", m.MessageID)
				logger.Debug("startIncomingComms: received pubrec", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				prel.MessageID = m.MessageID
				output <- incomingComms{outbound: &PacketAndToken{p: prel, t: nil}}
			case *packets.PubrelPacket:
				DEBUG.Println(NET, "startIncomingComms: received pubrel, id:", m.MessageID)
				logger.Debug("startIncomingComms: received pubrel", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
				pc.MessageID = m.MessageID
				c.persistOutbound(pc)
				output <- incomingComms{outbound: &PacketAndToken{p: pc, t: nil}}
			case *packets.PubcompPacket:
				DEBUG.Println(NET, "startIncomingComms: received pubcomp, id:", m.MessageID)
				logger.Debug("startIncomingComms: received pubcomp", slog.Uint64("messageID", uint64(m.MessageID)), componentAttr(NET))
				c.getToken(m.MessageID).flowComplete()
				c.freeID(m.MessageID)
			}
		}
	}()
	return output
}

// startOutgoingComms initiates a go routine to transmit outgoing packets.
// Pass in an open network connection and channels for outbound messages (including those triggered
// directly from incoming comms).
// Returns a channel that will receive details of any errors (closed when the goroutine exits)
// This function wil only terminate when all input channels are closed
func startOutgoingComms(conn net.Conn,
	c commsFns,
	oboundp <-chan *PacketAndToken,
	obound <-chan *PacketAndToken,
	oboundFromIncoming <-chan *PacketAndToken,
	logger *slog.Logger,
) <-chan error {
	errChan := make(chan error)
	DEBUG.Println(NET, "outgoing started")
	logger.Debug("outgoing started", componentAttr(NET))

	go func() {
		for {
			DEBUG.Println(NET, "outgoing waiting for an outbound message")
			logger.Debug("outgoing waiting for an outbound message", componentAttr(NET))

			// This goroutine will only exits when all of the input channels we receive on have been closed. This approach is taken to avoid any
			// deadlocks (if the connection goes down there are limited options as to what we can do with anything waiting on us and
			// throwing away the packets seems the best option)
			if oboundp == nil && obound == nil && oboundFromIncoming == nil {
				DEBUG.Println(NET, "outgoing comms stopping")
				logger.Debug("outgoing comms stopping", componentAttr(NET))
				close(errChan)
				return
			}

			select {
			case pub, ok := <-obound:
				if !ok {
					obound = nil
					continue
				}
				msg := pub.p.(*packets.PublishPacket)
				DEBUG.Println(NET, "obound msg to write", msg.MessageID)
				logger.Debug("obound msg to write", slog.Uint64("messageID", uint64(msg.MessageID)), componentAttr(NET))

				writeTimeout := c.getWriteTimeOut()
				if writeTimeout > 0 {
					if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
						ERROR.Println(NET, "SetWriteDeadline ", err)
						logger.Error("SetWriteDeadline error", slog.String("error", err.Error()), componentAttr(NET))
					}
				}

				if err := msg.Write(conn); err != nil {
					ERROR.Println(NET, "outgoing obound reporting error ", err)
					logger.Error("outgoing obound reporting error", slog.String("error", err.Error()), componentAttr(NET))
					pub.t.setError(err)
					// report error if it's not due to the connection being closed elsewhere
					if !strings.Contains(err.Error(), closedNetConnErrorText) {
						errChan <- err
					}
					continue
				}

				if writeTimeout > 0 {
					// If we successfully wrote, we don't want the timeout to happen during an idle period
					// so we reset it to infinite.
					if err := conn.SetWriteDeadline(time.Time{}); err != nil {
						ERROR.Println(NET, "SetWriteDeadline to 0 ", err)
						logger.Error("SetWriteDeadline to 0 error", slog.String("error", err.Error()), componentAttr(NET))
					}
				}

				if msg.Qos > 0 {
					c.checkAndSetFastReconnectCheckStartTime()
				}

				if msg.Qos == 0 {
					pub.t.flowComplete()
				}
				DEBUG.Println(NET, "obound wrote msg, id:", msg.MessageID)
				logger.Debug("obound wrote msg", slog.Uint64("messageID", uint64(msg.MessageID)), componentAttr(NET))
			case msg, ok := <-oboundp:
				if !ok {
					oboundp = nil
					continue
				}
				DEBUG.Println(NET, "obound priority msg to write, type", reflect.TypeOf(msg.p))
				logger.Debug("obound priority msg to write", slog.String("type", reflect.TypeOf(msg.p).String()), slog.Uint64("messageID", uint64(msg.p.Details().MessageID)), componentAttr(NET))
				if err := msg.p.Write(conn); err != nil {
					ERROR.Println(NET, "outgoing oboundp reporting error ", err)
					logger.Error("outgoing oboundp reporting error", slog.String("error", err.Error()), componentAttr(NET))
					if msg.t != nil {
						msg.t.setError(err)
					}
					errChan <- err
					continue
				}

				switch msg.p.(type) {
				case *packets.SubscribePacket, *packets.UnsubscribePacket:
					c.checkAndSetFastReconnectCheckStartTime()
				}

				if _, ok := msg.p.(*packets.DisconnectPacket); ok {
					msg.t.(*DisconnectToken).flowComplete()
					DEBUG.Println(NET, "outbound wrote disconnect, closing connection")
					logger.Debug("outbound wrote disconnect, closing connection", componentAttr(NET))
					// As per the MQTT spec "After sending a DISCONNECT Packet the Client MUST close the Network Connection"
					// Closing the connection will cause the goroutines to end in sequence (starting with incoming comms)
					_ = conn.Close()
				}
			case msg, ok := <-oboundFromIncoming: // message triggered by an inbound message (PubrecPacket or PubrelPacket)
				if !ok {
					oboundFromIncoming = nil
					continue
				}
				DEBUG.Println(NET, "obound from incoming msg to write, type", reflect.TypeOf(msg.p), " ID ", msg.p.Details().MessageID)
				logger.Debug("obound from incoming msg to write", slog.String("type", reflect.TypeOf(msg.p).String()), slog.Uint64("messageID", uint64(msg.p.Details().MessageID)), componentAttr(NET))

				if err := msg.p.Write(conn); err != nil {
					ERROR.Println(NET, "outgoing oboundFromIncoming reporting error", err)
					logger.Error("outgoing oboundFromIncoming reporting error", slog.String("error", err.Error()), componentAttr(NET))
					if msg.t != nil {
						msg.t.setError(err)
					}
					errChan <- err
					continue
				}

				switch msg.p.(type) {
				case *packets.PubrelPacket:
					c.checkAndSetFastReconnectCheckStartTime()
				}
			}
			c.UpdateLastSent() // Record that a packet has been received (for keepalive routine)
		}
	}()
	return errChan
}

// commsFns provide access to the client state (messageids, requesting disconnection and updating timing)
type commsFns interface {
	getToken(id uint16) tokenCompletor       // Retrieve the token for the specified messageid (if none then a dummy token must be returned)
	freeID(id uint16)                        // Release the specified messageid (clearing out of any persistent store)
	UpdateLastReceived()                     // Must be called whenever a packet is received
	UpdateLastSent()                         // Must be called whenever a packet is successfully sent
	getWriteTimeOut() time.Duration          // Return the writetimeout (or 0 if none)
	persistOutbound(m packets.ControlPacket) // add the packet to the outbound store
	persistInbound(m packets.ControlPacket)  // add the packet to the inbound store
	pingRespReceived()                       // Called when a ping response is received
	checkAndSetFastReconnectCheckStartTime() // Called when a packet that expects a response is sent
}

// startComms initiates goroutines that handles communications over the network connection
// Messages will be stored (via commsFns) and deleted from the store as necessary
// It returns two channels:
//
//	packets.PublishPacket - Will receive publish packets received over the network.
//	Closed when incoming comms routines exit (on shutdown or if network link closed)
//	error - Any errors will be sent on this channel. The channel is closed when all comms routines have shut down
//
// Note: The comms routines monitoring oboundp and obound will not shutdown until those channels are both closed. Any messages received between the
// connection being closed and those channels being closed will generate errors (and nothing will be sent). That way the chance of a deadlock is
// minimised.
func startComms(conn net.Conn, // Network connection (must be active)
	c commsFns, // getters and setters to enable us to cleanly interact with client
	inboundFromStore <-chan packets.ControlPacket, // Inbound packets from the persistence store (should be closed relatively soon after startup)
	oboundp <-chan *PacketAndToken,
	obound <-chan *PacketAndToken,
	logger *slog.Logger) (
	<-chan *packets.PublishPacket, // Publishpackages received over the network
	<-chan error, // Any errors (should generally trigger a disconnect)
) {
	// Start inbound comms handler; this needs to be able to transmit messages so we start a go routine to add these to the priority outbound channel
	ibound := startIncomingComms(conn, c, inboundFromStore, logger)
	outboundFromIncoming := make(chan *PacketAndToken) // Will accept outgoing messages triggered by startIncomingComms (e.g. acknowledgements)

	// Start the outgoing handler. It is important to note that output from startIncomingComms is fed into startOutgoingComms (for ACK's)
	oboundErr := startOutgoingComms(conn, c, oboundp, obound, outboundFromIncoming, logger)
	DEBUG.Println(NET, "startComms started")
	logger.Debug("startComms started", componentAttr(NET))

	// Run up go routines to handle the output from the above comms functions - these are handled in separate
	// go routines because they can interact (e.g. ibound triggers an ACK to obound which triggers an error)
	var wg sync.WaitGroup
	wg.Add(2)

	outPublish := make(chan *packets.PublishPacket)
	outError := make(chan error)

	// Any messages received get passed to the appropriate channel
	go func() {
		for ic := range ibound {
			if ic.err != nil {
				outError <- ic.err
				continue
			}
			if ic.outbound != nil {
				outboundFromIncoming <- ic.outbound
				continue
			}
			if ic.incomingPub != nil {
				outPublish <- ic.incomingPub
				continue
			}
			ERROR.Println(STR, "startComms received empty incomingComms msg")
			logger.Error("startComms received empty incomingComms msg", componentAttr(STR))
		}
		// Close channels that will not be written to again (allowing other routines to exit)
		close(outboundFromIncoming)
		close(outPublish)
		wg.Done()
	}()

	// Any errors will be passed out to our caller
	go func() {
		for err := range oboundErr {
			outError <- err
		}
		wg.Done()
	}()

	// outError is used by both routines so can only be closed when they are both complete
	go func() {
		wg.Wait()
		close(outError)
		DEBUG.Println(NET, "startComms closing outError")
		logger.Debug("startComms closing outError", componentAttr(NET))
	}()

	return outPublish, outError
}

// ackFunc acknowledges a packet
// WARNING the function returned must not be called if the comms routine is shutting down or not running
// (it needs outgoing comms in order to send the acknowledgement). Currently this is only called from
// matchAndDispatch which will be shutdown before the comms are
func ackFunc(oboundP chan *PacketAndToken, persist Store, packet *packets.PublishPacket, logger *slog.Logger) func() {
	return func() {
		switch packet.Qos {
		case 2:
			pr := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pr.MessageID = packet.MessageID
			DEBUG.Println(NET, "putting pubrec msg on obound")
			logger.Debug("putting pubrec msg on obound", componentAttr(NET))
			oboundP <- &PacketAndToken{p: pr, t: nil}
			DEBUG.Println(NET, "done putting pubrec msg on obound")
			logger.Debug("done putting pubrec msg on obound", componentAttr(NET))
		case 1:
			pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
			pa.MessageID = packet.MessageID
			DEBUG.Println(NET, "putting puback msg on obound")
			logger.Debug("putting puback msg on obound", componentAttr(NET))
			persistOutbound(persist, pa, logger)
			oboundP <- &PacketAndToken{p: pa, t: nil}
			DEBUG.Println(NET, "done putting puback msg on obound")
			logger.Debug("done putting puback msg on obound", componentAttr(NET))
		case 0:
			// do nothing, since there is no need to send an ack packet back
		}
	}
}
