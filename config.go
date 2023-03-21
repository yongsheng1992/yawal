package log

type SegmentConfig struct {
	MaxSegmentSize uint64
	MaxIndexSize   uint64
}

type Config struct {
	SegmentConfig SegmentConfig
}
