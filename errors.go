package yawal

import "errors"

var (
	ErrExceededMaxSegmentSize = errors.New("exceeded max segment size")
	ErrIllegalOffsetRange     = errors.New("offset is not in correct range")
)
