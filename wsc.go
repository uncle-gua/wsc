package wsc

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jpillora/backoff"
	"github.com/panjf2000/ants/v2"
)

var (
	ErrClose  = errors.New("connection closed")
	ErrBuffer = errors.New("message buffer is full")
)

type Wsc struct {
	// 配置信息
	Config *Config
	// 底层WebSocket
	WebSocket *WebSocket
	// 连接成功回调
	onConnected func()
	// 连接异常回调，在准备进行连接的过程中发生异常时触发
	onConnectError func(err error)
	// 连接断开回调，网络异常，服务端掉线等情况时触发
	onDisconnected func(err error)
	// 连接关闭回调，服务端发起关闭信号或客户端主动关闭时触发
	onClose func(code int, text string)

	// 发送Text消息成功回调
	onTextMessageSent func(message []byte)
	// 发送Binary消息成功回调
	onBinaryMessageSent func(data []byte)

	// 发送消息异常回调
	onSentError func(err error)

	// 接受到Ping消息回调
	onPingReceived func(appData string)
	// 接受到Pong消息回调
	onPongReceived func(appData string)
	// 接受到Text消息回调
	onTextMessageReceived func(message []byte)
	// 接受到Binary消息回调
	onBinaryMessageReceived func(data []byte)
	// 心跳
	onKeepalive func()
}

type Config struct {
	// 写超时
	WriteWait time.Duration
	// 支持接受的消息最大长度，默认512字节
	MaxMessageSize int64
	// 最小重连时间间隔
	MinRecTime time.Duration
	// 最大重连时间间隔
	MaxRecTime time.Duration
	// 每次重连失败继续重连的时间间隔递增的乘数因子，递增到最大重连时间间隔为止
	RecFactor float64
	// 消息发送缓冲池大小，默认256
	MessageBufferSize int
	// 心跳包时间间隔
	KeepaliveTime time.Duration
	// 允许断线重连
	EnableReconnect bool
}

type WebSocket struct {
	// 连接url
	Url           string
	Conn          *websocket.Conn
	Dialer        *websocket.Dialer
	RequestHeader http.Header
	HttpResponse  *http.Response
	// 是否已连接
	isConnected bool
	// 加锁避免重复关闭管道
	connMu *sync.RWMutex
	// 发送消息锁
	sendMu *sync.Mutex
	// 发送消息缓冲池
	sendChan chan *wsMsg
}

type wsMsg struct {
	t   int
	msg []byte
}

// New 创建一个Wsc客户端
func New(url string) *Wsc {
	return &Wsc{
		Config: &Config{
			WriteWait:         10 * time.Second,
			MaxMessageSize:    10 * 1024 * 1024,
			MinRecTime:        2 * time.Second,
			MaxRecTime:        60 * time.Second,
			RecFactor:         1.5,
			MessageBufferSize: 256,
			KeepaliveTime:     300,
			EnableReconnect:   true,
		},
		WebSocket: &WebSocket{
			Url:           url,
			Dialer:        websocket.DefaultDialer,
			RequestHeader: http.Header{},
			isConnected:   false,
			connMu:        &sync.RWMutex{},
			sendMu:        &sync.Mutex{},
		},
	}
}

func (wsc *Wsc) SetConfig(config *Config) {
	wsc.Config = config
}

func (wsc *Wsc) OnConnected(f func()) {
	wsc.onConnected = f
}

func (wsc *Wsc) OnConnectError(f func(err error)) {
	wsc.onConnectError = f
}

func (wsc *Wsc) OnDisconnected(f func(err error)) {
	wsc.onDisconnected = f
}

func (wsc *Wsc) OnClose(f func(code int, text string)) {
	wsc.onClose = f
}

func (wsc *Wsc) OnTextMessageSent(f func(message []byte)) {
	wsc.onTextMessageSent = f
}

func (wsc *Wsc) OnBinaryMessageSent(f func(data []byte)) {
	wsc.onBinaryMessageSent = f
}

func (wsc *Wsc) OnSentError(f func(err error)) {
	wsc.onSentError = f
}

func (wsc *Wsc) OnPingReceived(f func(appData string)) {
	wsc.onPingReceived = f
}

func (wsc *Wsc) OnPongReceived(f func(appData string)) {
	wsc.onPongReceived = f
}

func (wsc *Wsc) OnTextMessageReceived(f func(message []byte)) {
	wsc.onTextMessageReceived = f
}

func (wsc *Wsc) OnBinaryMessageReceived(f func(data []byte)) {
	wsc.onBinaryMessageReceived = f
}

func (wsc *Wsc) OnKeepalive(f func()) {
	wsc.onKeepalive = f
}

// IsConnected 返回连接状态
func (wsc *Wsc) IsConnected() bool {
	wsc.WebSocket.connMu.RLock()
	defer wsc.WebSocket.connMu.RUnlock()
	return wsc.WebSocket.isConnected
}

// Connect 发起连接
func (wsc *Wsc) Connect() {
	wsc.WebSocket.sendChan = make(chan *wsMsg, wsc.Config.MessageBufferSize) // 缓冲
	b := &backoff.Backoff{
		Min:    wsc.Config.MinRecTime,
		Max:    wsc.Config.MaxRecTime,
		Factor: wsc.Config.RecFactor,
		Jitter: true,
	}
	for {
		var err error
		nextRec := b.Duration()
		wsc.WebSocket.Conn, wsc.WebSocket.HttpResponse, err =
			wsc.WebSocket.Dialer.Dial(wsc.WebSocket.Url, wsc.WebSocket.RequestHeader)
		if err != nil {
			if wsc.onConnectError != nil {
				wsc.onConnectError(err)
			}
			// 重试
			time.Sleep(nextRec)
			continue
		}
		// 变更连接状态
		wsc.WebSocket.connMu.Lock()
		wsc.WebSocket.isConnected = true
		wsc.WebSocket.connMu.Unlock()
		// 连接成功回调
		if wsc.onConnected != nil {
			wsc.onConnected()
		}
		// 设置支持接受的消息最大长度
		wsc.WebSocket.Conn.SetReadLimit(wsc.Config.MaxMessageSize)
		// 收到连接关闭信号回调
		defaultCloseHandler := wsc.WebSocket.Conn.CloseHandler()
		wsc.WebSocket.Conn.SetCloseHandler(func(code int, text string) error {
			result := defaultCloseHandler(code, text)
			wsc.clean()
			if wsc.onClose != nil {
				wsc.onClose(code, text)
			}
			return result
		})
		// 收到ping回调
		defaultPingHandler := wsc.WebSocket.Conn.PingHandler()
		wsc.WebSocket.Conn.SetPingHandler(func(appData string) error {
			if wsc.onPingReceived != nil {
				wsc.onPingReceived(appData)
			}
			return defaultPingHandler(appData)
		})
		// 收到pong回调
		defaultPongHandler := wsc.WebSocket.Conn.PongHandler()
		wsc.WebSocket.Conn.SetPongHandler(func(appData string) error {
			if wsc.onPongReceived != nil {
				wsc.onPongReceived(appData)
			}
			return defaultPongHandler(appData)
		})
		// 开启协程读
		_ = ants.Submit(func() {
			wsc.writeLoop()
		})
		// 开启协程写
		_ = ants.Submit(func() {
			wsc.readLoop()
		})

		return
	}
}

// readLoop 消息读取
func (wsc *Wsc) readLoop() {
	for {
		messageType, message, err := wsc.WebSocket.Conn.ReadMessage()
		if err != nil {
			// 异常断线重连
			if wsc.onDisconnected != nil {
				wsc.onDisconnected(err)
			}
			wsc.closeAndRecConn()
			return
		}
		switch messageType {
		// 收到TextMessage回调
		case websocket.TextMessage:
			if wsc.onTextMessageReceived != nil {
				wsc.onTextMessageReceived(message)
			}
		// 收到BinaryMessage回调
		case websocket.BinaryMessage:
			if wsc.onBinaryMessageReceived != nil {
				wsc.onBinaryMessageReceived(message)
			}
		}
	}
}

// writeLoop 消息发送
func (wsc *Wsc) writeLoop() {
	keepaliveTick := time.NewTicker(wsc.Config.KeepaliveTime * time.Second)
	for {
		select {
		case wsMsg, ok := <-wsc.WebSocket.sendChan:
			if !ok {
				return
			}
			err := wsc.send(wsMsg.t, wsMsg.msg)
			if err != nil {
				if wsc.onSentError != nil {
					wsc.onSentError(err)
				}
				continue
			}
			switch wsMsg.t {
			case websocket.CloseMessage:
				return
			case websocket.TextMessage:
				if wsc.onTextMessageSent != nil {
					wsc.onTextMessageSent(wsMsg.msg)
				}
			case websocket.BinaryMessage:
				if wsc.onBinaryMessageSent != nil {
					wsc.onBinaryMessageSent(wsMsg.msg)
				}
			}
		case <-keepaliveTick.C:
			wsc.WebSocket.Conn.WriteMessage(websocket.PingMessage, nil)
			if wsc.onKeepalive != nil {
				wsc.onKeepalive()
			}
		}

	}
}

// SendTextMessage 发送TextMessage消息
func (wsc *Wsc) SendTextMessage(message string) error {
	if !wsc.IsConnected() {
		return ErrClose
	}
	// 丢入缓冲通道处理
	select {
	case wsc.WebSocket.sendChan <- &wsMsg{
		t:   websocket.TextMessage,
		msg: []byte(message),
	}:
	default:
		return ErrBuffer
	}
	return nil
}

// SendBinaryMessage 发送BinaryMessage消息
func (wsc *Wsc) SendBinaryMessage(data []byte) error {
	if !wsc.IsConnected() {
		return ErrClose
	}
	// 丢入缓冲通道处理
	select {
	case wsc.WebSocket.sendChan <- &wsMsg{
		t:   websocket.BinaryMessage,
		msg: data,
	}:
	default:
		return ErrBuffer
	}
	return nil
}

// send 发送消息到连接端
func (wsc *Wsc) send(messageType int, data []byte) error {
	wsc.WebSocket.sendMu.Lock()
	defer wsc.WebSocket.sendMu.Unlock()
	if !wsc.IsConnected() {
		return ErrClose
	}
	var err error
	// 超时时间
	_ = wsc.WebSocket.Conn.SetWriteDeadline(time.Now().Add(wsc.Config.WriteWait))
	err = wsc.WebSocket.Conn.WriteMessage(messageType, data)
	return err
}

// closeAndRecConn 断线重连
func (wsc *Wsc) closeAndRecConn() {
	if !wsc.IsConnected() {
		return
	}
	wsc.clean()
	if wsc.Config.EnableReconnect {
		_ = ants.Submit(func() {
			wsc.Connect()
		})
	}
}

// Close 主动关闭连接
func (wsc *Wsc) Close() {
	wsc.CloseWithMsg("")
}

// CloseWithMsg 主动关闭连接，附带消息
func (wsc *Wsc) CloseWithMsg(msg string) {
	if !wsc.IsConnected() {
		return
	}
	_ = wsc.send(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, msg))
	wsc.clean()
	if wsc.onClose != nil {
		wsc.onClose(websocket.CloseNormalClosure, msg)
	}
}

// clean 清理资源
func (wsc *Wsc) clean() {
	if !wsc.IsConnected() {
		return
	}
	wsc.WebSocket.connMu.Lock()
	wsc.WebSocket.isConnected = false
	_ = wsc.WebSocket.Conn.Close()
	close(wsc.WebSocket.sendChan)
	wsc.WebSocket.connMu.Unlock()
}
