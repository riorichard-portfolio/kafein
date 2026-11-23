package asyncq

type dirType string

const (
	wal       dirType = "wal"
	commited  dirType = "commited"
	corrupted dirType = "corrupted"
	temp      dirType = "temp"
)
