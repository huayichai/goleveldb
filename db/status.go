package db

type Code uint8

const (
	KOK              Code = 0
	KNotFound             = 1
	KCorruption           = 2
	KNotSupported         = 3
	KInvalidArgument      = 4
	KIOError              = 5
)

type Status struct {
	code Code
	msg  string
}

func NewStatus(c Code, msg string) Status {
	return Status{
		code: c,
		msg:  msg,
	}
}

func StatusOK() Status {
	return NewStatus(KOK, "")
}

func StatusNotFound(msg string) Status {
	return NewStatus(KNotFound, msg)
}

func StatusCorruption(msg string) Status {
	return NewStatus(KCorruption, "")
}

func StatusInvalidArgument(msg string) Status {
	return NewStatus(KInvalidArgument, "")
}

func StatusIOError(msg string) Status {
	return NewStatus(KIOError, "")
}

func (s Status) OK() bool {
	return s.code == KOK
}
