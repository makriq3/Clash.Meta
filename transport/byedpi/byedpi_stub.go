//go:build !cgo || (!linux && !android)

package byedpi

import "errors"

type Instance struct{}

func Start(args []string) (*Instance, error) {
	return nil, errors.New("byedpi backend requires cgo on linux/android")
}

func (i *Instance) Network() string {
	return ""
}

func (i *Instance) Addr() string {
	return ""
}

func (i *Instance) Close() error {
	return nil
}
