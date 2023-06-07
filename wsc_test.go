package wsc

import (
	"log"
	"testing"
)

func TestConnect(t *testing.T) {
	wsURL := "wss://danmuproxy.douyu.com:8501/"

	done := make(chan bool)
	ws := New(wsURL)
	// 可自定义配置，不使用默认配置
	//ws.SetConfig(&wsc.Config{
	//	// 写超时
	//	WriteWait: 10 * time.Second,
	//	// 支持接受的消息最大长度，默认512字节
	//	MaxMessageSize: 2048,
	//	// 最小重连时间间隔
	//	MinRecTime: 2 * time.Second,
	//	// 最大重连时间间隔
	//	MaxRecTime: 60 * time.Second,
	//	// 每次重连失败继续重连的时间间隔递增的乘数因子，递增到最大重连时间间隔为止
	//	RecFactor: 1.5,
	//	// 消息发送缓冲池大小，默认256
	//	MessageBufferSize: 1024,
	// 	// 心跳包时间间隔
	//	KeepaliveTime: 40 * time.Second,
	//	// 允许断线重连
	//	EnableReconnect: true,
	//})
	// 设置回调处理
	ws.OnConnected(func() {
		log.Println("OnConnected: ", ws.WebSocket.Url)
	})
	ws.OnConnectError(func(err error) {
		log.Println("OnConnectError: ", err.Error())
	})
	ws.OnDisconnected(func(err error) {
		log.Println("OnDisconnected: ", err.Error())
	})
	ws.OnClose(func(code int, text string) {
		log.Println("OnClose: ", code, text)
		done <- true
	})
	ws.OnTextMessageSent(func(message []byte) {
		log.Println("OnTextMessageSent: ", string(message))
	})
	ws.OnBinaryMessageSent(func(data []byte) {
		log.Println("OnBinaryMessageSent: ", string(data))
	})
	ws.OnSentError(func(err error) {
		log.Println("OnSentError: ", err.Error())
	})
	ws.OnPingReceived(func(appData string) {
		log.Println("OnPingReceived: ", appData)
	})
	ws.OnPongReceived(func(appData string) {
		log.Println("OnPongReceived: ", appData)
	})
	ws.OnTextMessageReceived(func(message []byte) {
		log.Println("OnTextMessageReceived: ", string(message))
	})
	ws.OnBinaryMessageReceived(func(data []byte) {
		log.Println("OnBinaryMessageReceived: ", string(data))
	})
	ws.OnKeepalive(func() {
		log.Println("OnKeepalive")
	})
	// 开始连接
	ws.Connect()
	for range done {
		return
	}
}
