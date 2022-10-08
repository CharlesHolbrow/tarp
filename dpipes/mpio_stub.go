//go:build !zmq
// +build !zmq

package dpipes

func ZMQSink(rawurl string, eof bool) func(Pipe) {
	panic("ZMQ unimplemented")

}
func ZMQSource(rawurl string, eof bool) func(Pipe) {
	panic("ZMQ unimplemented")
}
