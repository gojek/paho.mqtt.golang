package mqtt

import (
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gojek/paho.mqtt.golang/packets"
)

type mockBrokerConn struct {
	net.Conn
	clientConn   net.Conn
	brokerConn   net.Conn
	packetsSent  []packets.ControlPacket
	packetsRecvd []packets.ControlPacket
	mu           sync.Mutex
	paused       bool
	stopped      bool
}

func newMockBrokerConn() *mockBrokerConn {
	clientConn, brokerConn := net.Pipe()
	return &mockBrokerConn{
		Conn:         clientConn,
		clientConn:   clientConn,
		brokerConn:   brokerConn,
		packetsSent:  make([]packets.ControlPacket, 0),
		packetsRecvd: make([]packets.ControlPacket, 0),
	}
}

func (m *mockBrokerConn) runBroker() {
	go func() {
		defer m.brokerConn.Close()

		for {
			if m.stopped {
				return
			}
			packet, err := packets.ReadPacket(m.brokerConn)
			if err != nil {
				if !m.stopped {
					ERROR.Println(NET, "Mock broker read error:", err)
				}
				return
			}

			m.mu.Lock()
			m.packetsRecvd = append(m.packetsRecvd, packet)
			paused := m.paused
			m.mu.Unlock()

			DEBUG.Println(NET, "Mock broker received packet:", packet.String())

			if !paused {
				m.handlePacket(packet)
			}
		}
	}()
}

func (m *mockBrokerConn) handlePacket(packet packets.ControlPacket) {
	switch p := packet.(type) {
	case *packets.ConnectPacket:
		connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
		connack.ReturnCode = packets.Accepted
		connack.SessionPresent = false
		m.sendPacket(connack)

	case *packets.PublishPacket:
		switch p.Qos {
		case 1:
			puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
			puback.MessageID = p.MessageID
			m.sendPacket(puback)
		case 2:
			pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pubrec.MessageID = p.MessageID
			m.sendPacket(pubrec)
		}

	case *packets.PubrelPacket:
		pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pubcomp.MessageID = p.MessageID
		m.sendPacket(pubcomp)

	case *packets.SubscribePacket:
		suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
		suback.MessageID = p.MessageID
		suback.ReturnCodes = make([]byte, len(p.Topics))
		for i := range p.Topics {
			suback.ReturnCodes[i] = p.Qoss[i]
		}
		m.sendPacket(suback)

	case *packets.UnsubscribePacket:
		unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
		unsuback.MessageID = p.MessageID
		m.sendPacket(unsuback)

	case *packets.PingreqPacket:
		pingresp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
		m.sendPacket(pingresp)

	case *packets.DisconnectPacket:
		m.stopped = true
		return
	}
}

func (m *mockBrokerConn) sendPacket(packet packets.ControlPacket) {
	DEBUG.Println(NET, "Mock broker sending packet:", packet.String())
	err := packet.Write(m.brokerConn)
	if err != nil {
		ERROR.Println(NET, "Mock broker send error:", err)
		return
	}

	m.mu.Lock()
	m.packetsSent = append(m.packetsSent, packet)
	m.mu.Unlock()
}

func (m *mockBrokerConn) pauseResponses() {
	m.mu.Lock()
	m.paused = true
	m.mu.Unlock()
	DEBUG.Println(NET, "Mock broker responses paused")
}

func (m *mockBrokerConn) stop() {
	m.stopped = true
	m.clientConn.Close()
	m.brokerConn.Close()
}

func (m *mockBrokerConn) getReceivedPackets() []packets.ControlPacket {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]packets.ControlPacket, len(m.packetsRecvd))
	copy(result, m.packetsRecvd)
	return result
}

func createNewClientAndBroker(ackTimeout time.Duration, connectionLostChan chan bool) (Client, *mockBrokerConn) {
	mockBroker := newMockBrokerConn()
	mockBroker.runBroker()

	customConnFunc := func(uri *url.URL, options ClientOptions) (net.Conn, error) {
		return mockBroker.Conn, nil
	}

	opts := NewClientOptions().
		SetClientID("test-watchdog-client").
		SetCustomOpenConnectionFn(customConnFunc).
		SetAckTimeout(ackTimeout).
		SetKeepAlive(4 * time.Second).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(c Client, err error) {
			select {
			case connectionLostChan <- true:
			default:
			}
		}).
		AddBroker("tcp://mock-broker:1883")

	client := NewClient(opts)
	return client, mockBroker
}

func TestAckWatchdogPublishQoS1Timeout(t *testing.T) {
	ackTimeout := 1000 * time.Millisecond
	connectionLost := make(chan bool, 1)
	client, mockBroker := createNewClientAndBroker(ackTimeout, connectionLost)
	defer mockBroker.stop()

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	mockBroker.pauseResponses()

	pubToken := client.Publish("test/topic", 1, false, "test message")

	select {
	case <-connectionLost:
	case <-time.After(ackTimeout + 200*time.Millisecond):
		t.Fatal("Ack watchdog should have triggered within timeout period")
	}

	receivedPackets := mockBroker.getReceivedPackets()
	if len(receivedPackets) < 2 {
		t.Fatalf("Expected at least 2 packets (CONNECT, PUBLISH), got %d", len(receivedPackets))
	}

	foundPublish := false
	for _, packet := range receivedPackets {
		if _, ok := packet.(*packets.PublishPacket); ok {
			foundPublish = true
			break
		}
	}
	if !foundPublish {
		t.Fatal("PUBLISH packet not found in received packets")
	}

	if !pubToken.WaitTimeout(100 * time.Millisecond) {
		t.Fatal("Publish token should complete (with error) after connection loss")
	}

	client.Disconnect(250)
}

func TestAckWatchdogPublishQoS2Timeout(t *testing.T) {
	ackTimeout := 500 * time.Millisecond
	connectionLost := make(chan bool, 1)
	client, mockBroker := createNewClientAndBroker(ackTimeout, connectionLost)
	defer mockBroker.stop()

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	mockBroker.pauseResponses()

	pubToken := client.Publish("test/topic", 2, false, "test message qos2")

	select {
	case <-connectionLost:
	case <-time.After(ackTimeout + 200*time.Millisecond):
		t.Fatal("Ack watchdog should have triggered within timeout period")
	}

	receivedPackets := mockBroker.getReceivedPackets()
	foundPublish := false
	for _, packet := range receivedPackets {
		if pub, ok := packet.(*packets.PublishPacket); ok && pub.Qos == 2 {
			foundPublish = true
			break
		}
	}
	if !foundPublish {
		t.Fatal("QoS 2 PUBLISH packet not found in received packets")
	}

	if !pubToken.WaitTimeout(100 * time.Millisecond) {
		t.Fatal("Publish token should complete (with error) after connection loss")
	}

	client.Disconnect(250)
}

func TestAckWatchdogSubscribeTimeout(t *testing.T) {
	ackTimeout := 500 * time.Millisecond
	connectionLost := make(chan bool, 1)
	client, mockBroker := createNewClientAndBroker(ackTimeout, connectionLost)
	defer mockBroker.stop()

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	mockBroker.pauseResponses()

	subToken := client.Subscribe("test/topic", 1, nil)

	select {
	case <-connectionLost:
	case <-time.After(ackTimeout + 200*time.Millisecond):
		t.Fatal("Ack watchdog should have triggered within timeout period")
	}

	receivedPackets := mockBroker.getReceivedPackets()
	foundSubscribe := false
	for _, packet := range receivedPackets {
		if _, ok := packet.(*packets.SubscribePacket); ok {
			foundSubscribe = true
			break
		}
	}
	if !foundSubscribe {
		t.Fatal("SUBSCRIBE packet not found in received packets")
	}

	if !subToken.WaitTimeout(100 * time.Millisecond) {
		t.Fatal("Subscribe token should complete (with error) after connection loss")
	}

	client.Disconnect(250)
}

func TestAckWatchdogUnsubscribeTimeout(t *testing.T) {
	ackTimeout := 500 * time.Millisecond
	connectionLost := make(chan bool, 1)
	client, mockBroker := createNewClientAndBroker(ackTimeout, connectionLost)
	defer mockBroker.stop()

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	if token := client.Subscribe("test/topic", 1, nil); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to subscribe: %v", token.Error())
	}

	mockBroker.pauseResponses()

	unsubToken := client.Unsubscribe("test/topic")

	select {
	case <-connectionLost:
	case <-time.After(ackTimeout + 200*time.Millisecond):
		t.Fatal("Ack watchdog should have triggered within timeout period")
	}

	receivedPackets := mockBroker.getReceivedPackets()
	foundUnsubscribe := false
	for _, packet := range receivedPackets {
		if _, ok := packet.(*packets.UnsubscribePacket); ok {
			foundUnsubscribe = true
			break
		}
	}
	if !foundUnsubscribe {
		t.Fatal("UNSUBSCRIBE packet not found in received packets")
	}

	if !unsubToken.WaitTimeout(100 * time.Millisecond) {
		t.Fatal("Unsubscribe token should complete (with error) after connection loss")
	}

	client.Disconnect(250)
}

func TestAckWatchdogPingTimeout(t *testing.T) {
	connectionLost := make(chan bool, 1)
	mockBroker := newMockBrokerConn()
	mockBroker.runBroker()
	defer mockBroker.stop()

	customConnFunc := func(uri *url.URL, options ClientOptions) (net.Conn, error) {
		return mockBroker.Conn, nil
	}

	opts := NewClientOptions().
		SetClientID("test-ping-timeout").
		SetCustomOpenConnectionFn(customConnFunc).
		SetKeepAlive(1 * time.Second).
		SetAckTimeout(500 * time.Millisecond).
		SetPingTimeout(4 * time.Second).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(c Client, err error) {
			select {
			case connectionLost <- true:
			default:
			}
		}).
		AddBroker("tcp://mock-broker:1883")

	client := NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	mockBroker.pauseResponses()

	timeout := 2 * time.Second

	select {
	case <-connectionLost:
	case <-time.After(timeout):
		t.Fatal("Ping timeout should have triggered within timeout period")
	}

	receivedPackets := mockBroker.getReceivedPackets()
	foundPing := false
	for _, packet := range receivedPackets {
		if _, ok := packet.(*packets.PingreqPacket); ok {
			foundPing = true
			break
		}
	}
	if !foundPing {
		t.Fatal("PINGREQ packet not found in received packets")
	}

	client.Disconnect(250)
}

func TestAckWatchdogNormalFlow(t *testing.T) {
	ackTimeout := 500 * time.Millisecond
	connectionLost := make(chan bool, 1)
	_, mockBroker := createNewClientAndBroker(ackTimeout, connectionLost)
	defer mockBroker.stop()

	opts := NewClientOptions().
		SetClientID("test-watchdog-client").
		SetCustomOpenConnectionFn(func(uri *url.URL, options ClientOptions) (net.Conn, error) {
			return mockBroker.Conn, nil
		}).
		SetAckTimeout(ackTimeout).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(c Client, err error) {
			t.Errorf("Unexpected connection lost: %v", err)
			select {
			case connectionLost <- true:
			default:
			}
		}).
		AddBroker("tcp://mock-broker:1883")

	client := NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	for i := 0; i < 3; i++ {
		if token := client.Publish(fmt.Sprintf("test/topic/%d", i), 1, false, "test message"); token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to publish: %v", token.Error())
		}

		if token := client.Subscribe(fmt.Sprintf("test/sub/%d", i), 1, nil); token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to subscribe: %v", token.Error())
		}

		time.Sleep(100 * time.Millisecond)
	}

	select {
	case <-connectionLost:
		t.Fatal("Ack watchdog should not have triggered when responses are received")
	case <-time.After(ackTimeout + 200*time.Millisecond):
	}

	client.Disconnect(250)
}

func TestAckWatchdogDisabled(t *testing.T) {
	connectionLost := make(chan bool, 1)
	_, mockBroker := createNewClientAndBroker(0, connectionLost)
	defer mockBroker.stop()

	opts := NewClientOptions().
		SetClientID("test-watchdog-client").
		SetCustomOpenConnectionFn(func(uri *url.URL, options ClientOptions) (net.Conn, error) {
			return mockBroker.Conn, nil
		}).
		SetAckTimeout(0).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(c Client, err error) {
			t.Errorf("Unexpected connection lost: %v", err)
			select {
			case connectionLost <- true:
			default:
			}
		}).
		AddBroker("tcp://mock-broker:1883")

	client := NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect: %v", token.Error())
	}

	mockBroker.pauseResponses()

	pubToken := client.Publish("test/topic", 1, false, "test message")

	select {
	case <-connectionLost:
		t.Fatal("Ack watchdog should not trigger when disabled, AckTimeout = 0")
	case <-time.After(1 * time.Second):
	}

	if pubToken.WaitTimeout(50 * time.Millisecond) {
		t.Fatal("Publish token should still be waiting since broker is paused")
	}

	client.Disconnect(250)
}
