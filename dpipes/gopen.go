package dpipes

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

// TODO
// - make this table/registry driven
// - refactor into separate library
// - support common protocols directly via Go libraries

type waitCloser struct {
	io.ReadCloser
	cmd *exec.Cmd
}

func (c waitCloser) Close() error {
	c.ReadCloser.Close()
	return c.cmd.Wait()
}

// GOpen is a generic open that understands "-" and "pipe:" syntax.
func GOpen(fname string) (io.ReadCloser, error) {
	if fname == "-" {
		return os.Stdin, nil
	}
	if strings.HasPrefix(fname, "text:") {
		data := fname[5:]
		Debug.Println("text", data)
		return ioutil.NopCloser(strings.NewReader(data)), nil
	}
	if strings.HasPrefix(fname, "pipe:") {
		cmd := exec.Command("/bin/bash", "-c", fname[5:])
		Debug.Println("exec.Command", cmd)
		stream, err := cmd.StdoutPipe()
		Handle(err)
		stream2 := waitCloser{stream, cmd}
		cmd.Start()
		return stream2, err
	}
	fname = strings.TrimPrefix(fname, "file:")
	Debug.Println("open", fname)
	return os.Open(fname)
}

// ExternalCommand represents a process that can read tar files via it's stdin.
// In the example below, gsutil is the external ExternalCommand:
//
// tarp split all.tar --count 25 -o 'pipe:gsutil cp - gs://bucket/shard%06d.tar'
//
// tarp needs to stream the tar file into gsutil's stdin. Once we have finished
// writing the file to the external command's stdin, we must call stdin.Close()
// which will send the EOF character to gsutil, signaling the end of the input
// and allow gsutil to exit successfully.
//
// Remember,  golang requires that we create call cmd.StdinPipe BEFORE we call
// cmd.Start().
type ExternalCommand struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func (c ExternalCommand) Close() error {
	Debug.Println("CLOSING...")
	if closeError := c.stdin.Close(); closeError != nil {
		return closeError
	}

	waitError := c.cmd.Wait()
	Debug.Println("CLOSED!")
	return waitError
}

func (c ExternalCommand) Write(p []byte) (n int, err error) {
	Debug.Printf("Writing %d bytes", len(p))
	return c.stdin.Write(p)
}

// GCreate creates a io.WriteCloser that understands "-" and "pipe:" and
// filenames.
func GCreate(fname string) (io.WriteCloser, error) {
	if fname == "-" {
		return os.Stdout, nil
	}
	if strings.HasPrefix(fname, "pipe:") {
		cmd := exec.Command("/bin/bash", "-c", fname[5:])
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		stdinWriteCloser, stdinPipeErr := cmd.StdinPipe()

		if stdinPipeErr != nil {
			Debug.Println("Failed to create StdinPipe for", fname[5:])
			return nil, stdinPipeErr
		}

		if startError := cmd.Start(); startError != nil {
			Debug.Println("Failed to start command", fname[5:])
			return nil, startError
		}

		return ExternalCommand{cmd, stdinWriteCloser}, nil
	}

	fname = strings.TrimPrefix(fname, "file:")
	return os.Create(fname)
}

// WriteBinary writes the bytes to disk at fname.
func WriteBinary(fname string, data []byte) error {
	stream, err := GCreate(fname)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.Write(data)
	return err
}

// ReadBinary reads an entire file and returns a byte array.
func ReadBinary(fname string) ([]byte, error) {
	stream, err := GOpen(fname)
	if err != nil {
		return make([]byte, 0), err
	}
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	_, err = io.Copy(buffer, stream)
	if err != nil {
		return buffer.Bytes(), err
	}
	err = stream.Close()
	return buffer.Bytes(), err
}
