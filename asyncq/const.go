package asyncq

type dirType string

const (
	wal       dirType = "wal"
	commited  dirType = "commited"
	corrupted dirType = "corrupted"
	temp      dirType = "temp"
)

type fileType string

const (
	gobF       fileType = ".gob"
	gobFPoison fileType = ".gob.poison"
	gobFError  fileType = ".gob.error"
	poisonF    fileType = ".poison"
	errorF     fileType = ".error"
)
